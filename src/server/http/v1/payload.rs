use std::{collections::HashMap};

use serde::{Deserialize, Serialize};
use axum::{extract::Json, http::StatusCode};
use serde_json::Value;
 
#[derive(Deserialize)]
pub struct InsertIntoRequest {
    namespace: String,
    table: String,
    data: HashMap<String, Value>
}

#[derive(Serialize)]
pub struct Reponse {
    pub success: bool
}

pub async fn accept_payload(Json(payload): Json<InsertIntoRequest>) -> (StatusCode, Json<Reponse>) {
    println!("payload received for {}.{}; data length: {}", payload.namespace, payload.table, payload.data.len());
    let response = Reponse{
        success: true
    };

    (StatusCode::ACCEPTED, Json(response))
}