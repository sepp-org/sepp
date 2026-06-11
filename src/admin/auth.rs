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

use crate::config::{AdminKey, Role};
use crate::storage::now_ms;

use super::AdminState;

const COOKIE_NAME: &str = "sepp_admin";
// Live-session cap: past it, the soonest-to-expire session is evicted, so
// login spam with a valid key cannot grow the map without bound.
const MAX_SESSIONS: usize = 1024;
// Flat cost on failed logins; slows brute force without locking anyone out.
const FAILED_LOGIN_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

pub struct Session {
    pub key_name: String,
    pub key_sha256: [u8; 32],
    pub expires_ms: i64,
}

pub type SessionStore = Arc<RwLock<HashMap<String, Session>>>;

// Authenticated identity, stashed in request extensions by the middleware.
#[derive(Clone, Debug)]
pub struct AuthCtx {
    // None when auth is disabled (loopback zero-config mode).
    pub name: Option<String>,
    pub role: Role,
}

impl AuthCtx {
    pub fn actor(&self) -> &str {
        self.name.as_deref().unwrap_or("local")
    }
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

// Login matches name + key together. Every entry is still compared on both
// fields without early exit, so timing confirms neither which names exist nor
// which entry matched.
fn match_login<'a>(keys: &'a [AdminKey], name: &str, presented: &str) -> Option<&'a AdminKey> {
    let mut found = None;
    for key in keys {
        let name_ok = constant_time_eq(key.name.as_bytes(), name.as_bytes());
        let key_ok = constant_time_eq(key.key.as_bytes(), presented.as_bytes());
        if name_ok && key_ok {
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

// Secure tracks this listener's own TLS; behind a TLS-terminating proxy the
// hop to us is plain HTTP and the flag must stay off.
fn session_cookie(token: &str, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Strict; Path=/{secure}")
}

fn clear_cookie(secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{COOKIE_NAME}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0{secure}")
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
        // keys=None means auth off, which is only safe on a loopback bind.
        // The bound address is restart-only (boot is its truth), so a hot
        // edit that deletes the keys under a non-loopback listener fails
        // closed instead of silently opening the plane to the network.
        if state.boot.admin.listen_addr.ip().is_loopback() {
            req.extensions_mut().insert(AuthCtx {
                name: None,
                role: Role::Admin,
            });
            return next.run(req).await;
        }
        return unauthorized("admin keys were removed at runtime; restart required");
    };

    if let Some(presented) = bearer_key(req.headers())
        && let Some(matched) = match_key(keys, &presented)
    {
        req.extensions_mut().insert(AuthCtx {
            name: Some(matched.name.clone()),
            role: matched.role,
        });
        return next.run(req).await;
    }

    if let Some(token) = cookie_token(req.headers()) {
        let ctx = {
            let sessions = state.sessions.read().expect("session lock");
            sessions
                .get(&token)
                .filter(|s| s.expires_ms > now_ms())
                // Key rotation invalidates sessions: the stored hash must
                // still match a configured key with the same name. The role is
                // read from the live key, so hot role edits apply on the next
                // request instead of at the next login.
                .and_then(|s| {
                    keys.iter()
                        .find(|k| k.name == s.key_name && sha256(k.key.as_bytes()) == s.key_sha256)
                })
                .map(|k| AuthCtx {
                    name: Some(k.name.clone()),
                    role: k.role,
                })
        };
        if let Some(ctx) = ctx {
            req.extensions_mut().insert(ctx);
            return next.run(req).await;
        }
    }

    unauthorized("authentication required")
}

#[derive(Deserialize)]
pub struct LoginRequest {
    name: String,
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
        .and_then(|keys| match_login(keys, &body.name, &body.key))
    else {
        tracing::warn!(
            target: "sepp::audit",
            action = "session.login_failed",
            name = %body.name,
            "admin login failed: invalid name or key"
        );
        tokio::time::sleep(FAILED_LOGIN_DELAY).await;
        return unauthorized("invalid name or key");
    };

    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    // Saturating: validation bounds the TTL, but a hot-reloaded config must
    // never be able to panic the login handler.
    let expires_ms =
        now_ms().saturating_add(i64::try_from(config.admin.session_ttl_ms).unwrap_or(i64::MAX));
    let session = Session {
        key_name: matched.name.clone(),
        key_sha256: sha256(matched.key.as_bytes()),
        expires_ms,
    };

    {
        let mut sessions = state.sessions.write().expect("session lock");
        let now = now_ms();
        sessions.retain(|_, s| s.expires_ms > now);
        if sessions.len() >= MAX_SESSIONS
            && let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, s)| s.expires_ms)
                .map(|(t, _)| t.clone())
        {
            sessions.remove(&oldest);
        }
        sessions.insert(token.clone(), session);
    }

    tracing::info!(
        target: "sepp::audit",
        actor = %matched.name,
        role = %matched.role,
        action = "session.login",
        "admin login"
    );

    (
        [(
            header::SET_COOKIE,
            session_cookie(&token, state.boot.admin.tls_enabled()),
        )],
        Json(json!({
            "name": matched.name,
            "role": matched.role,
            "expires_at_ms": expires_ms,
        })),
    )
        .into_response()
}

pub async fn logout(State(state): State<Arc<AdminState>>, headers: HeaderMap) -> Response {
    if let Some(token) = cookie_token(&headers) {
        state.sessions.write().expect("session lock").remove(&token);
    }

    (
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            clear_cookie(state.boot.admin.tls_enabled()),
        )],
    )
        .into_response()
}

pub async fn session(
    State(state): State<Arc<AdminState>>,
    ctx: Option<axum::Extension<AuthCtx>>,
) -> Json<serde_json::Value> {
    let auth_enabled = state.config.load().admin.keys.is_some();
    match ctx {
        Some(axum::Extension(c)) => Json(json!({
            "name": c.name,
            "role": c.role,
            "auth_enabled": auth_enabled,
        })),
        None => Json(json!({ "name": null, "role": null, "auth_enabled": auth_enabled })),
    }
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
                role: Role::Admin,
            },
            AdminKey {
                name: "b".into(),
                key: "key-b".into(),
                role: Role::Viewer,
            },
        ];
        assert_eq!(
            match_key(&keys, "key-b").map(|k| k.name.as_str()),
            Some("b")
        );
        assert!(match_key(&keys, "nope").is_none());
    }

    #[test]
    fn match_login_requires_name_and_key_on_the_same_entry() {
        let keys = vec![
            AdminKey {
                name: "a".into(),
                key: "key-a".into(),
                role: Role::Admin,
            },
            AdminKey {
                name: "b".into(),
                key: "key-b".into(),
                role: Role::Viewer,
            },
        ];
        assert_eq!(
            match_login(&keys, "b", "key-b").map(|k| k.name.as_str()),
            Some("b")
        );
        assert!(match_login(&keys, "a", "key-b").is_none(), "crossed pair");
        assert!(match_login(&keys, "nope", "key-a").is_none());
        assert!(match_login(&keys, "a", "wrong").is_none());
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

    #[test]
    fn cookies_carry_secure_only_under_tls() {
        assert!(session_cookie("tok", true).ends_with("; Secure"));
        assert!(!session_cookie("tok", false).contains("Secure"));
        assert!(clear_cookie(true).ends_with("; Secure"));
        assert!(!clear_cookie(false).contains("Secure"));

        // The clear cookie must still expire the session cookie it replaces.
        assert!(clear_cookie(true).contains("Max-Age=0"));
    }
}
