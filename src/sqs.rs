use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sqs::{Client, Error, Region, PKG_VERSION};
use aws_sdk_sts::{Client as StsClient};
use aws_sdk_sqs::model::QueueAttributeName;
use tracing::{info, debug};
use serde_json::{json, Value};
use serde::{Serialize, Deserialize};
use dotenv::dotenv;

pub async fn create_simple_queue_service(queue_name: &str, bucket_name: &str) -> SimpleQueueService {
    let region_provider = RegionProviderChain::default_provider()
        .or_else(Region::new("us-east-1"));

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let region = String::from(&shared_config.region().unwrap().to_string());
    let client = Client::new(&shared_config);

    let sts_client = StsClient::new(&shared_config);
    let account_id = sts_client.get_caller_identity().send().await
        .expect("Could not get Caller Identity!").account.unwrap();

    let sqs = SimpleQueueService {
        client,
        region,
        account_id,
        bucket_name: bucket_name.to_string(),
        queue_name: queue_name.to_string(),
    };

    info!("Created simple queue service: {:?} with version: {:?}", &sqs, PKG_VERSION);

    sqs
}

pub struct SimpleQueueService {
    pub client: Client,
    pub region: String,
    pub account_id: String,
    pub queue_name: String,
    pub bucket_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct S3Object {
    pub key: String,
    pub size: u32,
    #[serde(rename = "eTag")]
    pub e_tag: String,
    pub sequencer: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct S3Bucket {
    pub name: String,
    #[serde(rename = "ownerIdentity")]
    pub owner_identity: Value,
    pub arn: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct S3Data {
    #[serde(rename = "s3SchemaVersion")]
    pub s3_schema_version: String,
    #[serde(rename = "configurationId")]
    pub configuration_id: String,
    pub bucket: S3Bucket,
    pub object: S3Object
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Record {
    #[serde(rename = "eventVersion")]
    pub event_version: String,
    #[serde(rename = "eventSource")]
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    #[serde(rename = "eventTime")]
    pub event_time: String,
    #[serde(rename = "eventName")]
    pub event_name: String,
    #[serde(rename = "userIdentity")]
    pub user_identity: Value,
    #[serde(rename = "requestParameters")]
    pub request_parameters: Value,
    #[serde(rename = "responseElements")]
    pub response_elements: Value,
    pub s3: S3Data,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    #[serde(rename = "Records")]
    pub records: Vec<Record>
}

impl std::fmt::Debug for SimpleQueueService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut formatter = f.debug_struct("Bucket");
        formatter.field("client", &self.client);
        formatter.field("region", &self.region);
        formatter.field("account_id", &self.account_id);
        formatter.field("queue_name", &self.queue_name);
        formatter.field("bucket_name", &self.bucket_name);
        formatter.finish()
    }
}

impl SimpleQueueService {
    pub fn get_queue_arn(&self) -> String {
        "arn:aws:sqs:".to_owned() + &self.region + ":" + &self.account_id + ":" + &self.queue_name
    }

    pub async fn create<'a>(&self) -> Result<String, Error> {
        info!("Creating message queue: {:?}...", &self.queue_name);
        let policy_json = json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowSQSS3BucketNotification",
                "Effect": "Allow",
                "Principal": {
                    "Service": "s3.amazonaws.com"
                },
                "Action": "sqs:SendMessage",
                "Resource": &self.get_queue_arn(),
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": "arn:aws:s3:::".to_owned() + &self.bucket_name
                    }
                }
            }]
        });

        info!("Policy JSON String: {:?}", policy_json.to_string());

        let create_queue_output = self.client.create_queue()
            .queue_name(&self.queue_name)
            .tags("project", "spendai")
            .attributes(QueueAttributeName::Policy, policy_json.to_string())
            .send()
            .await?;

        info!("create queue output: {:?}", create_queue_output);
        let queue_url = &*create_queue_output.queue_url.unwrap();

        Ok(String::from(queue_url))
    }

    pub async fn find_queue(&self, name: &str) -> Result<(), Error> {
        let queues = self.client.list_queues().send().await?;
        let queue_urls = queues.queue_urls().unwrap_or_default();
        info!("All queues in system: {:?}", queue_urls);
        let queue_url = queue_urls.iter()
            .filter(|url| url.ends_with(name))
            .last();
        info!("Found existing queue: {:?}", queue_url);
        Ok(())
    }

    pub async fn delete_queue(&self, queue_url: &str) -> Result<(), Error> {
        self.client.delete_queue().queue_url(queue_url).send().await?;
        Ok(())
    }

    pub async fn send(&self, queue_url: &str, message: &str) -> Result<(), Error> {
        let response = self.client
            .send_message()
            .queue_url(queue_url)
            .message_body(message)
            .send()
            .await?;
        debug!("Send response: {:?}", response);
        Ok(())
    }

    pub async fn receive(&self, queue_url: &str) -> Result<Option<Message>, Error> {
        info!("Receiving messages on with URL: `{}`", queue_url);

        let rcv_message_output = self.client
            .receive_message()
            .wait_time_seconds(20)
            .visibility_timeout(5)
            .queue_url(queue_url)
            .send().await?;

        let mut message_body: String = String::new();
        for message in rcv_message_output.messages.unwrap_or_default() {
            message_body.push_str(&message.body.unwrap().clone());
            info!("Got the message: {:#?}", &message_body);
            self.client
                .delete_message()
                .queue_url(queue_url)
                .receipt_handle(message.receipt_handle.unwrap())
                .send().await?;
            info!("Finished deletion...");
        }

        if message_body.is_empty(){
            return Ok(None)
        }

        let result: Result<Message, Error> = serde_json::from_str::<Message>(&message_body).
            map_err(|err| Error::Unhandled(Box::from(err)));

        result.map(|message| Some(message))
    }
}


#[tokio::main]
pub async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::init();
    dotenv().ok();

    let queue_name = "newqueuename4";
    let bucket_name = "groupingtest";

    let sqs = create_simple_queue_service(queue_name, bucket_name).await;

    let queue_url = sqs.create().await.unwrap();

    info!("**** queues *****");

    sqs.find_queue(&queue_url).await?;

    info!("**** receive *****");

    sqs.receive(&queue_url).await?;
    info!("**** send *****");
    sqs.send(&queue_url, "Hello world!").await?;

    Ok(())
}
