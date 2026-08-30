use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Extension, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Editor,
    Admin,
}

#[derive(Debug, Clone, Serialize)]
pub struct IdentityHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Principal {
    pub auth_method: &'static str,
    pub issuer: &'static str,
    pub kind: &'static str,
    pub subject: Option<String>,
    pub username: String,
    pub email: Option<String>,
    pub groups: Vec<String>,
    pub role: Role,
    pub headers: Vec<IdentityHeader>,
}

#[derive(Debug, Serialize)]
pub struct AuthSession {
    pub mode: &'static str,
    pub enabled: bool,
    pub subject: Option<String>,
    pub username: String,
    pub email: Option<String>,
    pub groups: Vec<String>,
    pub role: Role,
    pub headers: Vec<IdentityHeader>,
    pub logout_url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub enabled: bool,
    pub admin_group: String,
    pub editor_group: String,
    pub viewer_group: String,
    pub logout_url: Option<String>,
}

pub async fn require_viewer(
    State(config): State<Arc<AuthConfig>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    require_role(config, req, next, Role::Viewer).await
}

pub async fn require_editor(
    State(config): State<Arc<AuthConfig>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    require_role(config, req, next, Role::Editor).await
}

pub async fn require_admin(
    State(config): State<Arc<AuthConfig>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    require_role(config, req, next, Role::Admin).await
}

async fn require_role(
    config: Arc<AuthConfig>,
    mut req: Request<Body>,
    next: Next,
    required: Role,
) -> Response {
    let identity = if config.enabled {
        let Some(identity) = identity_from_headers(req.headers(), &config) else {
            tracing::warn!(
                method = %req.method(),
                path = %req.uri().path(),
                "Authorization denied: trusted proxy identity is missing or has no application role"
            );
            return (
                StatusCode::UNAUTHORIZED,
                "trusted proxy identity and an authorized role are required",
            )
                .into_response();
        };
        identity
    } else {
        local_admin_principal()
    };

    if identity.role < required {
        tracing::warn!(
            method = %req.method(),
            path = %req.uri().path(),
            subject = identity.subject.as_deref().unwrap_or("-"),
            username = %identity.username,
            role = ?identity.role,
            required_role = ?required,
            "Authorization denied: insufficient role"
        );
        return (StatusCode::FORBIDDEN, "insufficient role").into_response();
    }

    let method = req.method().clone();
    let path = req.uri().path().to_string();
    req.extensions_mut().insert(identity.clone());
    let response = next.run(req).await;
    if should_audit(&method, &path, required) {
        tracing::info!(
            target: "es_copy_indices_server::audit",
            method = %method,
            path = %path,
            status = response.status().as_u16(),
            auth_method = identity.auth_method,
            issuer = identity.issuer,
            kind = identity.kind,
            subject = identity.subject.as_deref().unwrap_or("-"),
            username = %identity.username,
            role = ?identity.role,
            "Authorized operation"
        );
    }
    response
}

fn should_audit(method: &Method, path: &str, required: Role) -> bool {
    (method != Method::GET && method != Method::HEAD && method != Method::OPTIONS)
        || (required == Role::Admin && (path.ends_with("/export") || path.contains("/configs/")))
}

fn local_admin_principal() -> Principal {
    Principal {
        auth_method: "unauthenticated",
        issuer: "local",
        kind: "user",
        subject: None,
        username: "Local administrator".to_string(),
        email: None,
        groups: Vec::new(),
        role: Role::Admin,
        headers: Vec::new(),
    }
}

fn identity_from_headers(headers: &HeaderMap, config: &AuthConfig) -> Option<Principal> {
    let username = first_header(headers, &["x-auth-user", "x-webauth-user"])?;
    let subject = first_header(headers, &["x-auth-subject", "x-webauth-subject"]);
    let email = first_header(headers, &["x-auth-email", "x-webauth-email"]);
    let groups = parse_groups(
        &first_header(headers, &["x-auth-groups", "x-webauth-groups"]).unwrap_or_default(),
    );
    let role = role_from_groups(&groups, config)?;

    Some(Principal {
        auth_method: "trusted_proxy_headers",
        issuer: "proxy",
        kind: "user",
        subject,
        username,
        email,
        groups,
        role,
        headers: visible_identity_headers(headers),
    })
}

fn first_header(headers: &HeaderMap, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| headers.get(*name))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_groups(input: &str) -> Vec<String> {
    input
        .split(|c: char| c == ',' || c == ';' || c.is_whitespace())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn role_from_groups(groups: &[String], config: &AuthConfig) -> Option<Role> {
    if groups.iter().any(|group| group == &config.admin_group) {
        Some(Role::Admin)
    } else if groups.iter().any(|group| group == &config.editor_group) {
        Some(Role::Editor)
    } else if groups.iter().any(|group| group == &config.viewer_group) {
        Some(Role::Viewer)
    } else {
        None
    }
}

fn visible_identity_headers(headers: &HeaderMap) -> Vec<IdentityHeader> {
    const VISIBLE_HEADERS: [(&str, &str); 8] = [
        ("x-auth-subject", "X-Auth-Subject"),
        ("x-auth-user", "X-Auth-User"),
        ("x-auth-email", "X-Auth-Email"),
        ("x-auth-groups", "X-Auth-Groups"),
        ("x-webauth-subject", "X-WEBAUTH-SUBJECT"),
        ("x-webauth-user", "X-WEBAUTH-USER"),
        ("x-webauth-email", "X-WEBAUTH-EMAIL"),
        ("x-webauth-groups", "X-WEBAUTH-GROUPS"),
    ];

    VISIBLE_HEADERS
        .iter()
        .filter_map(|(lookup, display)| {
            headers
                .get(*lookup)
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| IdentityHeader {
                    name: (*display).to_string(),
                    value: value.to_string(),
                })
        })
        .collect()
}

pub async fn session(
    Extension(config): Extension<Arc<AuthConfig>>,
    Extension(identity): Extension<Principal>,
) -> Json<AuthSession> {
    let session = AuthSession {
        mode: if config.enabled {
            "trusted-proxy"
        } else {
            "unauthenticated"
        },
        enabled: config.enabled,
        subject: identity.subject,
        username: identity.username,
        email: identity.email,
        groups: identity.groups,
        role: identity.role,
        headers: identity.headers,
        logout_url: if config.enabled {
            config.logout_url.clone()
        } else {
            None
        },
    };
    Json(session)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn config() -> AuthConfig {
        AuthConfig {
            enabled: true,
            admin_group: "es-copy-indices:admin".to_string(),
            editor_group: "es-copy-indices:editor".to_string(),
            viewer_group: "es-copy-indices:viewer".to_string(),
            logout_url: Some("/logout".to_string()),
        }
    }

    #[test]
    fn canonical_headers_take_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("x-auth-subject", HeaderValue::from_static("subject-1"));
        headers.insert("x-auth-user", HeaderValue::from_static("canonical"));
        headers.insert("x-webauth-user", HeaderValue::from_static("alias"));
        headers.insert(
            "x-auth-groups",
            HeaderValue::from_static("es-copy-indices:admin"),
        );

        let identity = identity_from_headers(&headers, &config()).unwrap();
        assert_eq!(identity.username, "canonical");
        assert_eq!(identity.subject.as_deref(), Some("subject-1"));
        assert_eq!(identity.role, Role::Admin);
    }

    #[test]
    fn webauth_aliases_and_groups_are_accepted() {
        let mut headers = HeaderMap::new();
        headers.insert("x-webauth-user", HeaderValue::from_static("operator"));
        headers.insert(
            "x-webauth-groups",
            HeaderValue::from_static("es-copy-indices:editor"),
        );

        let identity = identity_from_headers(&headers, &config()).unwrap();
        assert_eq!(identity.username, "operator");
        assert_eq!(identity.role, Role::Editor);
    }

    #[test]
    fn highest_group_role_wins() {
        let mut headers = HeaderMap::new();
        headers.insert("x-auth-user", HeaderValue::from_static("operator"));
        headers.insert(
            "x-auth-groups",
            HeaderValue::from_static(
                "es-copy-indices:viewer, es-copy-indices:editor;es-copy-indices:admin",
            ),
        );

        let identity = identity_from_headers(&headers, &config()).unwrap();
        assert_eq!(identity.role, Role::Admin);
    }

    #[test]
    fn missing_identity_or_role_is_rejected() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-auth-groups",
            HeaderValue::from_static("es-copy-indices:viewer"),
        );
        assert!(identity_from_headers(&headers, &config()).is_none());

        let mut headers = HeaderMap::new();
        headers.insert("x-auth-user", HeaderValue::from_static("operator"));
        assert!(identity_from_headers(&headers, &config()).is_none());
    }

    #[test]
    fn audit_covers_mutations_and_sensitive_admin_reads() {
        assert!(should_audit(&Method::POST, "/runs", Role::Editor));
        assert!(should_audit(
            &Method::GET,
            "/runs/run-1/export",
            Role::Admin
        ));
        assert!(should_audit(
            &Method::GET,
            "/runs/run-1/configs/job.toml",
            Role::Admin
        ));
        assert!(!should_audit(&Method::GET, "/status", Role::Viewer));
    }
}
