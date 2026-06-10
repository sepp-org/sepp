//! Role enforcement for the admin API. Identity arrives as an `AuthCtx`
//! request extension (inserted by `auth::require`); handlers declare the role
//! they need by taking one of the extractors below as a parameter, so a route
//! without an extractor stands out in review.
//!
//! Roles are strictly ordered: viewer < operator < admin.
//!   viewer    every GET: dashboards, queue/job inspection, redacted config, SSE
//!   operator  job-level mutations: enqueue, dead-letter, requeue, delete dead letters
//!   admin     config-level mutations: queue create/update/delete, config edits

use axum::extract::FromRequestParts;
use axum::http::StatusCode;
use axum::http::request::Parts;
use serde_json::Value;

use crate::config::Role;

use super::auth::AuthCtx;
use super::routes::ApiError;

// Read-only handlers take RequireViewer purely for the access check, so its
// identity field is intentionally unread.
pub(crate) struct RequireViewer(#[allow(dead_code)] pub AuthCtx);
pub(crate) struct RequireOperator(pub AuthCtx);
pub(crate) struct RequireAdmin(pub AuthCtx);

fn extract(parts: &Parts, required: Role) -> Result<AuthCtx, ApiError> {
    let Some(ctx) = parts.extensions.get::<AuthCtx>() else {
        // Unreachable behind auth::require; fails closed if a route is ever
        // registered outside the middleware by mistake.
        return Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "authentication required",
        ));
    };
    if ctx.role < required {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("this action requires the {required} role"),
        ));
    }
    Ok(ctx.clone())
}

impl<S: Send + Sync> FromRequestParts<S> for RequireViewer {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        extract(parts, Role::Viewer).map(Self)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RequireOperator {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        extract(parts, Role::Operator).map(Self)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for RequireAdmin {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        extract(parts, Role::Admin).map(Self)
    }
}

// One line per successful mutation, attributed to the acting key. Routed by
// target so operators can split the audit trail off into its own sink.
pub fn audit(ctx: &AuthCtx, action: &str, details: Value) {
    tracing::info!(
        target: "sepp::audit",
        actor = %ctx.actor(),
        role = %ctx.role,
        action,
        details = %details,
        "admin action"
    );
}

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;

    use super::*;

    fn parts_with(ctx: Option<AuthCtx>) -> Parts {
        let mut req = axum::http::Request::builder()
            .body(())
            .expect("build request");
        if let Some(c) = ctx {
            req.extensions_mut().insert(c);
        }
        req.into_parts().0
    }

    fn ctx(role: Role) -> AuthCtx {
        AuthCtx {
            name: Some("t".into()),
            role,
        }
    }

    fn status_of(err: ApiError) -> StatusCode {
        err.into_response().status()
    }

    #[test]
    fn the_role_matrix_is_strictly_ordered() {
        let cases = [
            (Role::Viewer, Role::Viewer, true),
            (Role::Viewer, Role::Operator, false),
            (Role::Viewer, Role::Admin, false),
            (Role::Operator, Role::Viewer, true),
            (Role::Operator, Role::Operator, true),
            (Role::Operator, Role::Admin, false),
            (Role::Admin, Role::Viewer, true),
            (Role::Admin, Role::Operator, true),
            (Role::Admin, Role::Admin, true),
        ];
        for (have, need, allowed) in cases {
            let parts = parts_with(Some(ctx(have)));
            let result = extract(&parts, need);
            assert_eq!(
                result.is_ok(),
                allowed,
                "role {have} requesting {need} should be allowed={allowed}"
            );
            if let Err(e) = result {
                assert_eq!(status_of(e), StatusCode::FORBIDDEN);
            }
        }
    }

    #[test]
    fn a_missing_identity_fails_closed_with_401() {
        let parts = parts_with(None);
        let err = extract(&parts, Role::Viewer).expect_err("no identity, no access");
        assert_eq!(status_of(err), StatusCode::UNAUTHORIZED);
    }
}
