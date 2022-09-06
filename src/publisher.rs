use tracing_subscriber;
use tracing::{info};
use std::env;
use serde_json::json;

use deadpool_lapin::{Config, Pool, Runtime};
use deadpool_lapin::lapin::{
    types::FieldTable, BasicProperties, options::{BasicPublishOptions, QueueDeclareOptions}
};

use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();
    dotenv().ok();

    let uri: String = env::var("BROKER_URL").unwrap();
    let queue_name: String = env::var("QUEUE_NAME").unwrap();
    info!("Using uri: {} with queue: {}", &uri, &queue_name);

    let mut cfg = Config::default();
    cfg.url = Some(uri.clone());
    let pool: Pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let connection = pool.get().await?;
    let channel = connection.create_channel().await.unwrap();

    let _queue = channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let payload = json!({
        "namespace": "default",
        "model": "empty-job",
        "bucket": "groupingtest",
        "vendors_input": "test/input.csv",
        "mapping_file": "test/mapping_file.dat",
        "vendors_output": "test/grouped.csv",
        "vendors_output_detailed": "test/grouped_datascience.csv"
    });

    channel
        .basic_publish(
            "",
            &queue_name,
            BasicPublishOptions::default(),
            payload.to_string().as_bytes(),
            BasicProperties::default(),
        )
        .await
        .expect("basic_publish"); // Drop the PublisherConfirm instead for waiting for it ...
    Ok(())

}
