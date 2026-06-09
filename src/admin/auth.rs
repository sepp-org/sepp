use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use axum::Json;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::AdminKey;
use crate::storage::now_ms;

use super::AdminState;

const COOKIE_NAME: &str = "sepp_admin";

pub struct Session {
    pub key_name: String,
    pub key_sha256: [u8; 32],
    pub expires_ms: i64,
}

pub type SessionStore = Arc<RwLock<HashMap<String, Session>>>;

// Authenticated identity, stashed in request extensions by the middleware.
#[derive(Clone)]
pub struct Identity {
    pub name: Option<String>,
}

// Compares without short-circuiting on the first mismatching byte.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= (x ^ y) as usize;
    }
    diff == 0
}

// Every configured key is compared so timing does not reveal which one matched.
fn match_key<'a>(keys: &'a [AdminKey], presented: &str) -> Option<&'a AdminKey> {
    let mut found = None;
    for key in keys {
        if constant_time_eq(key.key.as_bytes(), presented.as_bytes()) {
            found = Some(key);
        }
    }
    found
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn cookie_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (name, value) = pair.trim().split_once('=')?;
        (name == COOKIE_NAME).then(|| value.to_string())
    })
}

fn bearer_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::to_string)
}

fn unauthorized(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({ "error": message, "code": "unauthorized" })),
    )
        .into_response()
}

pub async fn require(
    State(state): State<Arc<AdminState>>,
    mut req: Request,
    next: Next,
) -> Response {
    if req.method() == Method::POST && req.uri().path() == "/admin/api/v1/session" {
        return next.run(req).await;
    }

    let config = state.config.load();
    let Some(keys) = config.admin.keys.as_deref() else {
        req.extensions_mut().insert(Identity { name: None });
        return next.run(req).await;
    };

    if let Some(presented) = bearer_key(req.headers())
        && let Some(matched) = match_key(keys, &presented)
    {
        let name = matched.name.clone();
        req.extensions_mut().insert(Identity { name: Some(name) });
        return next.run(req).await;
    }

    if let Some(token) = cookie_token(req.headers()) {
        let name = {
            let sessions = state.sessions.read().expect("session lock");
            sessions
                .get(&token)
                .filter(|s| s.expires_ms > now_ms())
                // Key rotation invalidates sessions: the stored hash must
                // still match a configured key with the same name.
                .filter(|s| {
                    keys.iter().any(|k| {
                        k.name == s.key_name && sha256(k.key.as_bytes()) == s.key_sha256
                    })
                })
                .map(|s| s.key_name.clone())
        };
        if let Some(name) = name {
            req.extensions_mut().insert(Identity { name: Some(name) });
            return next.run(req).await;
        }
    }

    unauthorized("authentication required")
}

#[derive(Deserialize)]
pub struct LoginRequest {
    key: String,
}

pub async fn login(
    State(state): State<Arc<AdminState>>,
    Json(body): Json<LoginRequest>,
) -> Response {
    let config = state.config.load();
    let Some(matched) = config
        .admin
        .keys
        .as_deref()
        .and_then(|keys| match_key(keys, &body.key))
    else {
        return unauthorized("invalid admin key");
    };

    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let expires_ms = now_ms() + config.admin.session_ttl_ms as i64;
    let session = Session {
        key_name: matched.name.clone(),
        key_sha256: sha256(matched.key.as_bytes()),
        expires_ms,
    };

    {
        let mut sessions = state.sessions.write().expect("session lock");
        let now = now_ms();
        sessions.retain(|_, s| s.expires_ms > now);
        sessions.insert(token.clone(), session);
    }

    (
        [(
            header::SET_COOKIE,
            format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/"),
        )],
        Json(json!({ "name": matched.name, "expires_at_ms": expires_ms })),
    )
        .into_response()
}

pub async fn logout(State(state): State<Arc<AdminState>>, headers: HeaderMap) -> Response {
    if let Some(token) = cookie_token(&headers) {
        state
            .sessions
            .write()
            .expect("session lock")
            .remove(&token);
    }

    (
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            format!("{COOKIE_NAME}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0"),
        )],
    )
        .into_response()
}

pub async fn session(
    State(state): State<Arc<AdminState>>,
    identity: Option<axum::Extension<Identity>>,
) -> Json<serde_json::Value> {
    let auth_enabled = state.config.load().admin.keys.is_some();
    let name = identity.and_then(|axum::Extension(i)| i.name);
    Json(json!({ "name": name, "auth_enabled": auth_enabled }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_handles_unequal_lengths() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secret2"));
        assert!(!constant_time_eq(b"secret", b"secres"));
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn match_key_finds_the_matching_name() {
        let keys = vec![
            AdminKey {
                name: "a".into(),
                key: "key-a".into(),
            },
            AdminKey {
                name: "b".into(),
                key: "key-b".into(),
            },
        ];
        assert_eq!(match_key(&keys, "key-b").map(|k| k.name.as_str()), Some("b"));
        assert!(match_key(&keys, "nope").is_none());
    }

    #[test]
    fn cookie_token_parses_the_sepp_admin_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            "other=1; sepp_admin=tok123; theme=dark".parse().unwrap(),
        );
        assert_eq!(cookie_token(&headers), Some("tok123".to_string()));

        let mut missing = HeaderMap::new();
        missing.insert(header::COOKIE, "other=1".parse().unwrap());
        assert_eq!(cookie_token(&missing), None);
        assert_eq!(cookie_token(&HeaderMap::new()), None);
    }
}
