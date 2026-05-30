use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tonic::service::Interceptor;
use tonic::{Request, Status};
use tracing::warn;

#[derive(Clone)]
pub struct ApiKeyInterceptor {
    // None = auth disabled, allow all. Some(set) = only those keys pass;
    // an empty set rejects everyone.
    policy: Arc<RwLock<Option<HashSet<String>>>>,
}

impl ApiKeyInterceptor {
    pub fn new(api_keys: &Option<Vec<String>>) -> Self {
        Self {
            policy: Arc::new(RwLock::new(to_policy(api_keys.clone()))),
        }
    }

    pub fn set_keys(&self, api_keys: Option<Vec<String>>) {
        *self.policy.write().expect("api-key policy lock") = to_policy(api_keys);
    }

    pub fn is_enforcing(&self) -> bool {
        self.policy.read().expect("api-key policy lock").is_some()
    }
}

fn to_policy(api_keys: Option<Vec<String>>) -> Option<HashSet<String>> {
    api_keys.map(|keys| keys.into_iter().collect())
}

impl Interceptor for ApiKeyInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let policy = self.policy.read().expect("api-key policy lock");
        let Some(allowed) = policy.as_ref() else {
            return Ok(request);
        };

        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));

        match presented {
            Some(key) if allowed.contains(key) => Ok(request),
            Some(_) => {
                warn!("rejected request: invalid API key");
                Err(Status::unauthenticated("invalid API key"))
            }
            None => {
                warn!("rejected request: missing API key");
                Err(Status::unauthenticated(
                    "missing API key; expected an `authorization: Bearer <key>` header",
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_with(auth: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(value) = auth {
            req.metadata_mut()
                .insert("authorization", value.parse().unwrap());
        }
        req
    }

    #[test]
    fn absent_list_allows_everything() {
        let mut interceptor = ApiKeyInterceptor::new(&None);
        assert!(!interceptor.is_enforcing());
        assert!(interceptor.call(request_with(None)).is_ok());
    }

    #[test]
    fn empty_list_rejects_everything() {
        let mut interceptor = ApiKeyInterceptor::new(&Some(vec![]));
        assert!(interceptor.is_enforcing());
        assert!(interceptor.call(request_with(None)).is_err());
        assert!(
            interceptor
                .call(request_with(Some("Bearer anything")))
                .is_err()
        );
    }

    #[test]
    fn non_empty_list_admits_only_listed_keys() {
        let mut interceptor = ApiKeyInterceptor::new(&Some(vec!["good".to_string()]));
        assert!(interceptor.call(request_with(Some("Bearer good"))).is_ok());
        assert!(interceptor.call(request_with(Some("Bearer bad"))).is_err());
        assert!(interceptor.call(request_with(Some("good"))).is_err()); // no scheme
        assert!(interceptor.call(request_with(None)).is_err());
    }

    #[test]
    fn keys_can_be_replaced_at_runtime() {
        let interceptor = ApiKeyInterceptor::new(&Some(vec!["old".to_string()]));
        // A separate clone stands in for the copy tonic hands each request.
        let mut handle = interceptor.clone();
        assert!(handle.call(request_with(Some("Bearer old"))).is_ok());

        // An update through one handle is visible to the other clone.
        interceptor.set_keys(Some(vec!["new".to_string()]));
        assert!(handle.call(request_with(Some("Bearer old"))).is_err());
        assert!(handle.call(request_with(Some("Bearer new"))).is_ok());

        // Auth can be switched off entirely at runtime.
        interceptor.set_keys(None);
        assert!(!handle.is_enforcing());
        assert!(handle.call(request_with(None)).is_ok());
    }
}
