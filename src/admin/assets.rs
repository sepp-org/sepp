use axum::Json;
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Redirect, Response};
use rust_embed::RustEmbed;
use serde_json::json;

#[derive(RustEmbed)]
#[folder = "admin-ui/dist"]
struct Assets;

pub async fn serve(uri: Uri) -> Response {
    let path = uri.path();
    if path == "/" {
        return Redirect::temporary("/admin/").into_response();
    }

    let not_found = || {
        (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "not found", "code": "not_found" })),
        )
    };

    if path.starts_with("/admin/api/") {
        return not_found().into_response();
    }
    let Some(rest) = path.strip_prefix("/admin") else {
        return not_found().into_response();
    };

    let rel = rest.trim_start_matches('/');
    if !rel.is_empty()
        && let Some(file) = Assets::get(rel)
    {
        // Vite content-hashes everything under assets/, so those never change;
        // everything else (index.html, favicons) must revalidate.
        let cache = if rel.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        };
        return (
            [
                (header::CONTENT_TYPE, mime_of(rel)),
                (header::CACHE_CONTROL, cache),
            ],
            file.data.into_owned(),
        )
            .into_response();
    }

    // SPA history routing: any other /admin path serves the app shell.
    match Assets::get("index.html") {
        Some(file) => (
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            file.data.into_owned(),
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "admin UI assets missing").into_response(),
    }
}

fn mime_of(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "json" | "map" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
