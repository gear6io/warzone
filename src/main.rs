mod server;

use crate::server::http::serve;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    serve().await
}
