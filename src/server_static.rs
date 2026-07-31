use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

const TABLER_CSS: &[u8] = include_bytes!("../static/vendor/tabler/css/tabler.min.css");
const TABLER_JS: &[u8] = include_bytes!("../static/vendor/tabler/js/tabler.min.js");
const TABLER_ICONS_CSS: &[u8] =
    include_bytes!("../static/vendor/tabler-icons/css/tabler-icons.min.css");
const TABLER_ICONS_WOFF: &[u8] =
    include_bytes!("../static/vendor/tabler-icons/css/fonts/tabler-icons.woff");
const TABLER_ICONS_WOFF2: &[u8] =
    include_bytes!("../static/vendor/tabler-icons/css/fonts/tabler-icons.woff2");
const FAVICON_SVG: &[u8] = include_bytes!("../static/favicon.svg");

pub async fn serve(Path(path): Path<String>) -> Response {
    let path = path.trim_start_matches('/');
    let asset = match path {
        "vendor/tabler/css/tabler.min.css" => Some((TABLER_CSS, "text/css; charset=utf-8")),
        "vendor/tabler/js/tabler.min.js" => Some((TABLER_JS, "text/javascript; charset=utf-8")),
        "vendor/tabler-icons/css/tabler-icons.min.css" => {
            Some((TABLER_ICONS_CSS, "text/css; charset=utf-8"))
        }
        "vendor/tabler-icons/css/fonts/tabler-icons.woff" => Some((TABLER_ICONS_WOFF, "font/woff")),
        "vendor/tabler-icons/css/fonts/tabler-icons.woff2" => {
            Some((TABLER_ICONS_WOFF2, "font/woff2"))
        }
        "favicon.svg" => Some((FAVICON_SVG, "image/svg+xml")),
        _ => None,
    };

    let Some((content, content_type)) = asset else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let mut response = Response::new(Body::from(content));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=3600"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn known_asset_is_served_with_content_type() {
        let response = serve(Path("vendor/tabler/css/tabler.min.css".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/css; charset=utf-8"
        );

        let response = serve(Path("favicon.svg".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/svg+xml"
        );
    }

    #[tokio::test]
    async fn unknown_asset_is_not_found() {
        let response = serve(Path("missing.css".to_string())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
