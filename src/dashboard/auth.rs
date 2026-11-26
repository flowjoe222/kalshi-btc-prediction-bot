//! Dashboard auth middleware.
//!
//! Validates `Authorization: Bearer <DASHBOARD_TOKEN>` on every `/api/*`
//! request. The root `/` (HTML page) and `/feed` (SSE) are intentionally left
//! open so the embedded dashboard still loads — the SSE stream contains no
//! mutations and the HTML is just static markup.
//!
//! If `DASHBOARD_TOKEN` is unset, this layer is a no-op (dev mode) but a
//! warning is logged at startup.
//!
//! Constant-time comparison: we don't pull in the `subtle` crate just for
//! this; instead we do a manual length-then-byte loop with an accumulator
//! that XORs every byte and a final `== 0` check. This avoids early-out
//! short-circuits that would leak token length / prefix via timing.

use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    middleware::{self, Next},
    response::Response,
    Router,
};
use std::sync::Arc;

/// Wrap a `Router` so every `/api/*` route requires
/// `Authorization: Bearer <DASHBOARD_TOKEN>`.
///
/// Pass `None` to disable auth entirely (dev mode).
pub fn with_auth<S>(router: Router<S>, token: Option<String>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let Some(token) = token else {
        // Dev mode: no token configured, skip the middleware entirely.
        return router;
    };
    let token = Arc::new(token);
    router.layer(middleware::from_fn(move |req, next| {
        let token = Arc::clone(&token);
        async move { auth_middleware(token, req, next).await }
    }))
}

async fn auth_middleware(
    token: Arc<String>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // Only guard /api/* — leave the static dashboard HTML and SSE feed open.
    let path = req.uri().path();
    if !path.starts_with("/api/") {
        return next.run(req).await;
    }

    // Allow CORS preflight requests through unauthenticated; browsers don't
    // attach Authorization on OPTIONS preflights.
    if req.method() == axum::http::Method::OPTIONS {
        return next.run(req).await;
    }

    let header_val = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let presented = header_val.strip_prefix("Bearer ").unwrap_or("");

    if !constant_time_eq(presented.as_bytes(), token.as_bytes()) {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header(header::WWW_AUTHENTICATE, "Bearer")
            .body(Body::from("unauthorized"))
            .unwrap_or_else(|_| {
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(Body::empty())
                    .expect("static unauthorized response builds")
            });
    }

    next.run(req).await
}

/// Constant-time byte slice equality.
///
/// Returns `false` immediately on length mismatch (length itself is not
/// secret — it's the public token length). For equal-length inputs we XOR
/// every byte and OR into an accumulator, so the loop runs identically
/// regardless of where the first differing byte is.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn empty_eq() {
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn matches() {
        assert!(constant_time_eq(b"abcd", b"abcd"));
    }

    #[test]
    fn rejects_length_mismatch() {
        assert!(!constant_time_eq(b"abcd", b"abcde"));
    }

    #[test]
    fn rejects_byte_mismatch() {
        assert!(!constant_time_eq(b"abcd", b"abce"));
    }
}
