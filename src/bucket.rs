use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::model::{BucketLocationConstraint, CreateBucketConfiguration, Event, NotificationConfiguration, QueueConfiguration};
use aws_sdk_s3::{Client, Error, PKG_VERSION};
use aws_sdk_s3::output::{CreateBucketOutput, GetBucketNotificationConfigurationOutput, HeadBucketOutput};
use tracing::{info};
use dotenv::dotenv;

pub async fn create_bucket_manager(bucket_name: &str) -> BucketManager {
    let region_provider = RegionProviderChain::default_provider()
        .or_else("us-east-1");

    let config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&config);
    let region = String::from(&config.region().unwrap().to_string());

    let bucket_manager = BucketManager {
        client,
        bucket_name: bucket_name.to_string(),
        region,
    };

    info!("Created bucket manager: {:?} with version: {:?}", &bucket_manager, PKG_VERSION);

    bucket_manager
}

pub struct BucketManager {
    pub client: Client,
    pub bucket_name: String,
    pub region: String,
}

impl std::fmt::Debug for BucketManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut formatter = f.debug_struct("BucketManager");
        formatter.field("client", &self.client);
        formatter.field("bucket_name", &self.bucket_name);
        formatter.field("region", &self.region);
        formatter.finish()
    }
}

impl BucketManager {
    pub async fn exists2(&self) -> Result<bool, Error> {
        let result: HeadBucketOutput = self.client
            .head_bucket()
            .bucket(&self.bucket_name)
            .send()
            .await?;
        info!("exists: {:?}", result);
        Ok(true)
    }

    pub async fn exists(&self) -> Result<bool, Error> {
        let resp = self.client.list_buckets().send().await?;
        let buckets = resp.buckets().unwrap_or_default();

        for bucket in buckets {
            if self.bucket_name.eq(bucket.name().unwrap()) {
                info!("Bucket found: {:?}", bucket.name().unwrap_or_default());
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub async fn list_all(&self) -> Result<(), Error> {
        let resp = self.client.list_buckets().send().await?;
        let buckets = resp.buckets().unwrap_or_default();

        for bucket in buckets {
            info!("Bucket found: {:?}", bucket.name().unwrap_or_default());
        }
        Ok(())
    }

    pub async fn create(&self) -> Result<CreateBucketOutput, Error> {
        let mut create_bucket_builder = self.client
            .create_bucket()
            .bucket(&self.bucket_name);

        if self.region.ne("us-east-1") {
            let constraint = BucketLocationConstraint::from(self.region.as_str());
            let cfg = CreateBucketConfiguration::builder()
                .location_constraint(constraint)
                .build();
            create_bucket_builder = create_bucket_builder.create_bucket_configuration(cfg);
        }

        let created_bucket = create_bucket_builder.send().await?;
        info!("Created bucket: {:?}", &self.bucket_name);

        Ok(created_bucket)
    }

    pub async fn set_notifications(&self, bucket: &str, queue_arn: &str, queue_name: &str)
                                   -> Result<GetBucketNotificationConfigurationOutput, aws_sdk_s3::Error> {
        let notification_configuration = NotificationConfiguration::builder()
            .queue_configurations(QueueConfiguration::builder()
                .queue_arn(queue_arn)
                .id(queue_name)
                .events(Event::from("s3:ObjectCreated:Put".to_string()))
                .events(Event::from("s3:ObjectCreated:Post".to_string()))
                .build()
            ).build();

        let put_bucket_config_result = self.client
            .put_bucket_notification_configuration()
            .notification_configuration(notification_configuration)
            .bucket(bucket).send().await?;

        info!("Put bucket configuration result: {:?}", put_bucket_config_result);

        let get_bucket_config_result = self.client
            .get_bucket_notification_configuration()
            .bucket(bucket)
            .send().await?;

        info!("Bucket notification configuration: {:?}", get_bucket_config_result);

        return Ok(get_bucket_config_result);
    }
}

#[tokio::main]
pub async fn main() -> Result<(), aws_sdk_s3::Error> {
    tracing_subscriber::fmt::init();
    dotenv().ok();

    let bucket_manager = create_bucket_manager("wade-workingdirx2").await;

    bucket_manager.list_all().await?;
    Ok(())
}
