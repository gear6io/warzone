use axum::{Json, http::{StatusCode}};
use serde::Serialize;

#[derive(Serialize)]
pub struct HealthCheckResponse  {
    ready : bool
}

pub async fn ready() -> (StatusCode, Json<HealthCheckResponse>) {
    (StatusCode::OK, Json(HealthCheckResponse { ready: true }))
}