use tracing_subscriber;
use tracing::{info, debug};
use futures_lite::stream::StreamExt;
use std::{env};
use std::collections::HashMap;
use std::future::Future;
use dotenv::dotenv;
use kube::core::ErrorResponse;
use serde_json::{Result};
use k8s_openapi::api::batch::v1::JobStatus;
use deadpool_lapin::{Config, Pool, Runtime};
use deadpool_lapin::lapin::{types::FieldTable, BasicProperties, options::{BasicPublishOptions, QueueDeclareOptions}, Channel};
use deadpool_lapin::lapin::options::{BasicAckOptions, BasicConsumeOptions};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct StatusMessage {
    pub name: String,
    #[serde(rename = "inputLocation")]
    pub input_location: Option<String>,
    #[serde(rename = "outputLocation")]
    pub output_location: Option<String>,
    pub action: String,
    pub status: Option<JobStatus>,
    pub environment: Option<HashMap<String, String>>,
    pub error: Option<ErrorResponse>
}

pub fn create_queue_manager(uri: &str) -> QueueManager {
    let mut cfg = Config::default();
    cfg.url = Some(uri.to_string());
    let pool: Pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();
    let queue_manager: QueueManager = QueueManager { pool };
    queue_manager
}

#[derive(Clone)]
pub struct QueueManager {
    pool: Pool,
}

impl std::fmt::Debug for QueueManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut formatter = f.debug_struct("QueueManager");
        formatter.field("pool", &self.pool.status());
        formatter.finish()
    }
}

impl QueueManager {

    pub async fn get_channel(&self) -> Channel {
        let connection = self.pool.get().await.unwrap();
        connection.create_channel().await.unwrap()
    }

    pub async fn create(&self, queue_name: &str) -> Result<()> {
        info!("Creating queue: {}", queue_name);
        let channel = self.get_channel().await;
        let _queue = channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions::default(),
                FieldTable::default(),
            ).await
            .expect("Could not create message queue!");

        Ok(())
    }

    pub async fn publish(&self, queue_name: &str, message: &str) -> Result<()> {
        info!("Sending to queue: {}, full message: {}", queue_name, message);
        let channel = self.get_channel().await;
        channel
            .basic_publish(
                "",
                &queue_name,
                BasicPublishOptions::default(),
                message.to_string().as_bytes(),
                BasicProperties::default(),
            )
            .await
            .expect("Problem sending message to queue!");
        Ok(())
    }

    pub async fn start_listening<F,Fut>(&self, queue_name: &str, action: F) -> anyhow::Result<()>
        where
            F: FnOnce(String, QueueManager) -> Fut + std::marker::Copy,
            Fut: Future<Output=anyhow::Result<()>> {
        let connection = self.pool.get()
            .await
            .expect("RabbitMQ Connection Error!");

        let channel = connection.create_channel().await
            .expect("Could not create channel listener!");

        debug!("Creating consumer...");
        let mut consumer = channel
            .basic_consume(
                &queue_name,
                "model_executor_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .expect("error in basic_consume");

        info!(state=?connection.status().state());

        while let Some(delivery) = consumer.next().await {
            info!(message=?delivery, "received message");
            let message = delivery.as_ref().unwrap();
            let data = String::from_utf8(message.data.clone()).unwrap();
            info!("Message data: {:?}", &data);
            info!("Message Routing Key: {:?}", &message.routing_key);

            let _result = action(data.clone(), self.clone()).await?;
            info!("Result of embedded action before acknowledging message: {:?}", _result);
            if let Ok(delivery) = delivery {
                debug!("Acknowledging delivery of message...");
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("basic_ack");
            }
        }
        Ok(())
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    dotenv().ok();
    const START_POSTFIX: &str = "-start";
    const STATUS_POSTFIX: &str = "-status";

    let uri: String = env::var("BROKER_URL").unwrap();
    let namespace: String = env::var("NAMESPACE").unwrap();

    let queue_manager = create_queue_manager(uri.as_str());

    queue_manager.create((namespace.to_owned() + START_POSTFIX).as_str()).await?;
    queue_manager.create((namespace.to_owned() + STATUS_POSTFIX).as_str()).await?;

    let queue_name = namespace.to_owned() + STATUS_POSTFIX;

    info!("Using uri: {} with queue: {}", &uri, &queue_name);

    queue_manager.create(queue_name.as_str()).await?;

    let status = StatusMessage {
        name: "job-name".to_string(),
        input_location: Some("environment/model-name/input.csv".to_string()),
        output_location: Some("environment/model-name/output.csv".to_string()),
        action: "waiting".to_string(),
        status: None,
        environment: None,
        error: None
    };

    let status_json = serde_json::to_string(&status).unwrap();

    queue_manager.publish(queue_name.as_str(), status_json.to_string().as_str()).await?;

    info!("current queue manager state: {:?}", queue_manager);

    Ok(())
}
