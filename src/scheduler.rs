use std::collections::HashMap;
use tracing::{info, debug, error, warn};
use std::{env, fs, path::Path};
use std::sync::Arc;
use chrono::{DateTime, Utc};
use dotenv::dotenv;
use futures::{StreamExt, TryStreamExt};
use kube::api::WatchEvent;
use k8s_openapi::api::batch::v1::{Job, JobStatus};
use serde_json::{Result};
use tokio::task::JoinHandle;
use tokio::time;
use crate::sqs::{create_simple_queue_service, SimpleQueueService};
use crate::bucket::{create_bucket_manager, BucketManager};
use crate::queue::{create_queue_manager, QueueManager};
use k8s_openapi::api::core::v1::EnvVar;
use kube::ResourceExt;
use std::{thread};
use kube::error::ErrorResponse;

use crate::job::Watched;
use crate::queue::StatusMessage;

pub mod bucket;
pub mod job;
pub mod sqs;
pub mod queue;

const MODEL: &str = "model";
const INPUT_LOCATION: &str = "input_location";
const OUTPUT_LOCATION: &str = "output_location";
const START_POSTFIX: &str = "-start";
const STATUS_POSTFIX: &str = "-status";
const VGQC_POSTFIX: &str = "-vgqc-start";
const INPUT_FILE_POSTFIX: &str = "/input.csv";

async fn retransmit_bucket_events(queue_url: Arc<String>, simple_queue_service: Arc<SimpleQueueService>, queue_manager: Arc<QueueManager>) -> anyhow::Result<()> {
    loop {
        let result = simple_queue_service.receive(&queue_url[..]).await;
        debug!("Received: {:?}", &result);
        if result.is_ok() {
            let message = result.unwrap();
            if message.is_some() {
                let data = message.unwrap();
                for record in data.records.iter() {
                    info!("Working through record: {:?}", &record);
                    let s3_data = &record.s3;
                    let s3_object = &s3_data.object;
                    let location = &s3_object.key;
                    if location.ends_with(INPUT_FILE_POSTFIX) {
                        let location_parts = location.split("/").collect::<Vec<_>>();
                        if location_parts.len() >= 2 {
                            let namespace = location_parts[0];
                            let model = location_parts[1];

                            let start_queue = if model.eq("vgqc") {namespace.to_owned() + VGQC_POSTFIX} else { namespace.to_owned() + START_POSTFIX};
                            let mut contents: HashMap<String, String> = HashMap::new();
                            contents.insert(MODEL.to_string(), model.to_string());
                            contents.insert(INPUT_LOCATION.to_string(), location.to_string());

                            info!("New input detected for: {:?} in namespace: {:?} published to queue: {:?}", model, namespace, &start_queue);
                            queue_manager.publish(&start_queue, serde_json::to_string(&contents).unwrap().as_str()).await?;
                        } else {
                            warn!("Received change on non working location: {:?}", location);
                        }
                    }
                    info!("Location: {:?}", &location);
                }
            }
        } else {
            warn!("Could not parse sqs message: {:?}", &result);
        }
        time::sleep(time::Duration::from_secs(10)).await;
        debug!("Checking for SQS messages...");

        debug!("Forwarding messages to rabbitmq...");
    }
}

async fn watch_jobs(namespace: Arc<String>, queue_manager: Arc<QueueManager>) -> anyhow::Result<()> {
    let mut stream = job::get_job_streams(&namespace).await.boxed();
    let start_time = Utc::now();
    let status_queue = namespace[..].to_string() + STATUS_POSTFIX;
    queue_manager.create(&status_queue).await
        .expect("Could not create status queue for namespace!");
    loop {
        let result = stream.try_next().await;
        let o = if let Some(o) = result.unwrap() { o } else {
            let ten_seconds = time::Duration::from_secs(10);
            warn!("Could not retrieve job stream! Trying again in 10 seconds!");
            thread::sleep(ten_seconds);
            continue;
        };
        match o {
            Watched::Job(job) => {
                let status: JobStatus = job.status.clone().unwrap();
                let _spec = job.spec.clone().unwrap();
                let completion_time = status.completion_time.clone();
                let env = _spec.template.spec.unwrap().containers[0].env.clone();

                let input_location: Option<String> = env.clone().and_then(| envs | envs.iter()
                    .filter(|env_var| env_var.name == INPUT_LOCATION.to_string().to_uppercase())
                    .map(|env_var| env_var.value.clone().unwrap()).nth(0));

                let output_location: Option<String> = env.clone().and_then(| envs | envs.iter()
                    .filter(|env_var| env_var.name == OUTPUT_LOCATION.to_string().to_uppercase())
                    .map(|env_var| env_var.value.clone().unwrap()).nth(0));

                let env_map = env.clone().map(| envs | HashMap::from_iter(envs.iter()
                    .filter(|env_var| env_var.value.is_some())
                    .map(|env_var| (env_var.name.clone(), env_var.value.clone().unwrap()))));

                let create_status_message= |action, job: Job | {
                    let job_status = StatusMessage {
                        name: job.name(),
                        input_location,
                        output_location,
                        action,
                        status: job.status,
                        environment: env_map,
                        error: None
                    };
                    let status_json = serde_json::to_string(&job_status).unwrap();
                    status_json
                };

                if completion_time.is_some() {
                    let completion_date: DateTime<Utc> = completion_time.unwrap().0;
                    debug!("Ignore {:?} compared to: {:?}", completion_date, start_time);
                    if start_time < completion_date && status.active.is_none() {
                        let status_json = create_status_message("complete".to_string(), job.clone());
                        debug!("Status: {}", &status_json);
                        queue_manager.publish(&status_queue, &status_json.as_str()).await
                            .expect("Could not publish added job to queue!");
                    } else {
                        debug!("Discarded event for pre existing job: {:?}", job.name());
                    }
                } else {
                    if status.active.is_some() && status.active.unwrap() == 1 {
                        info!("Found newly scheduled job : {:?}", job.name());
                        let status_json = create_status_message("running".to_string(), job.clone());
                        debug!("Status: {}", &status_json);
                        queue_manager.publish(&status_queue, &status_json.as_str()).await
                            .expect("Could not publish added job to queue!");
                    } else if status.failed.is_some() {
                        info!("Found newly created job: {:?}", job.name());
                        let status_json = create_status_message("failed".to_string(), job.clone());
                        debug!("Status: {}", &status_json);
                        queue_manager.publish(&status_queue, &status_json.as_str()).await
                            .expect("Could not publish added job to queue!");
                    } else {
                        info!("Found newly created job: {:?}", job.name());
                        let status_json = create_status_message("waiting".to_string(), job.clone());
                        debug!("Status: {}", &status_json);
                        queue_manager.publish(&status_queue, &status_json.as_str()).await
                            .expect("Could not publish added job to queue!");
                    }
                }
                info!("Processed: {:?}", job.name());
            }
        }
    }
}

async fn setup_job_listeners(namespace: &str, queue_manager: QueueManager) -> JoinHandle<anyhow::Result<()>>{
    let queue_manager_arc: Arc<QueueManager> = Arc::new(queue_manager);
    let namesapce_arc: Arc<String> = Arc::new(namespace.to_string());
    tokio::spawn(watch_jobs(namesapce_arc, queue_manager_arc))
}

async fn setup_queue_listeners(bucket_name: &str, sqs_queue_name: &str, queue_manager: QueueManager) -> JoinHandle<anyhow::Result<()>> {
    //create queues, connect them to the bucket and start listening to events
    let bucket_manager: BucketManager = create_bucket_manager(bucket_name).await;
    let simple_queue_service = create_simple_queue_service(sqs_queue_name, bucket_name).await;
    if !bucket_manager.exists().await.unwrap() {
        bucket_manager.create().await
            .expect("Could not create s3 bucket");
    }
    let queue_url = simple_queue_service.create().await
        .expect("Could not create sqs queue!");
    let queue_arn = simple_queue_service.get_queue_arn();
    bucket_manager.set_notifications(bucket_name, queue_arn.as_str(), sqs_queue_name).await
        .expect("Could not add notifications to s3 bucket!");
    //spawn a thread that checks the sqs messages and publishes messages on arrival
    let simple_queue_service_arc: Arc<SimpleQueueService> = Arc::new(simple_queue_service);
    let queue_manager_arc: Arc<QueueManager> = Arc::new(queue_manager);
    let queue_url_arc: Arc<String> = Arc::new(queue_url);

    tokio::spawn(retransmit_bucket_events(queue_url_arc, simple_queue_service_arc, queue_manager_arc))
}

async fn execute_model_job(data: String, queue_manager: QueueManager) -> anyhow::Result<()> {
    let namespace: String = env::var("NAMESPACE").expect("NAMESPACE is not set as an environment variable!");
    let cleanup_min: usize = env::var("CLEANUP_MIN").expect("CLEANUP_MIN is not set as an environment variable!").parse().unwrap();

    let result: Result<HashMap<String, String>> = serde_json::from_str(&data);
    if result.is_ok() {
        let contents: &HashMap<String, String> = result.as_ref().unwrap();
        info!("Message contents: {:?}", &contents);
        if contents.contains_key(MODEL) && contents.contains_key(INPUT_LOCATION) {
            info!("Preparing job in namespace: {:?}", &namespace);
            let model = contents.get(MODEL).unwrap();
            let input_location = contents.get(INPUT_LOCATION).unwrap().to_string();

            info!("Running job for model: {:?} in namespace: {:?}", model, namespace);
            let mut env_vars: Vec<EnvVar> = contents.iter()
                .map(|(k, v)| EnvVar { name: k.to_string(), value: Some(v.to_string()), value_from: None })
                .collect();

            env_vars.push(EnvVar { name: INPUT_LOCATION.to_string().to_uppercase(), value: Some(input_location.to_string()), value_from: None });

            let create_error_message = | error | {
                let job_status = StatusMessage {
                    name: model.to_string(),
                    output_location: None,
                    input_location: Some(input_location.to_string()),
                    action: "error".to_owned(),
                    status: None,
                    environment: None,
                    error: Some(error)
                };
                let status_json = serde_json::to_string(&job_status).unwrap();
                status_json
            };

            let status_queue = namespace.to_owned() + STATUS_POSTFIX;

            let new_job_result = job::get_job_from_cronjob(model, &namespace, env_vars).await;
            if new_job_result.is_err() {
                let error = new_job_result.unwrap_err();
                let api_error: ErrorResponse = error.downcast().unwrap();
                warn!("Anyhow error from kubernetes api: {:?}", &api_error);
                let status_json = create_error_message(api_error.clone());
                queue_manager.publish(status_queue.as_str(), &status_json.as_str()).await
                     .expect("Could not publish error to queue!");
                return Err(anyhow::Error::from(api_error));
            }

            let new_job = new_job_result.unwrap();
            info!("Executing job: {:?}", new_job.name());
            let mut stream = job::execute(new_job, &namespace).await.boxed();
            info!("Execution started, retrieving stream results from api...");

            queue_manager.create(&status_queue).await
                .expect("Could not create status queue for namespace!");

            while let Some(status) = stream.try_next().await.unwrap() {
                match status {
                    WatchEvent::Added(s) => {
                        info!("Added {}", &s.name());
                    }
                    WatchEvent::Modified(s) => {
                        let current_status = s.status.clone().expect("Status is missing");
                        match current_status.completion_time {
                            Some(_) => {
                                info!("Modified: {} is complete", s.name());
                                break;
                            }
                            _ => info!("Modified: {} is running", s.name()),
                        }
                    }
                    WatchEvent::Deleted(s) => {
                        info!("Deleted {}", s.name())
                    }
                    WatchEvent::Error(s) => {
                        error!("{:?}", s);
                        let status_json = create_error_message(s);
                        queue_manager.publish(status_queue.as_str(), &status_json.as_str()).await
                            .expect("Could not publish error to queue!");
                    }
                    _ => {}
                }
            }

            job::cleanup(&*(model.to_owned() + "-"), &namespace, cleanup_min).await
                .expect("Kubernetes job cleanup failed!");

            info!("Finished job launch in namespace: {:?} with result: {:?}", &namespace, &result);
        } else {
            error!("Cant run model, because model name or input_location is missing in arguments!");
        }
    } else {
        error!("Unable to parse json: {:?}", &result.err());
    }
    Ok(())
}

async fn create_namespaced_queues(queue_manager: &QueueManager, namespace: &String) -> anyhow::Result<()> {
    queue_manager.create((namespace.to_owned() + START_POSTFIX).as_str()).await?;
    queue_manager.create((namespace.to_owned() + VGQC_POSTFIX).as_str()).await?;
    queue_manager.create((namespace.to_owned() + STATUS_POSTFIX).as_str()).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = env::args().collect();

    if args.iter().count() > 1 {
        let environment_file = &args[1];
        info!("Loading environment settings from: {} exists: {}", environment_file, Path::new(environment_file).exists());
        let contents = fs::read_to_string(environment_file).expect("Cant read the environment file");
        info!("With text:\n{}", contents);
        dotenv::from_filename(environment_file).ok();
    } else {
        dotenv().ok();
    }

    let broker_uri: String = env::var("BROKER_URL").expect("BROKER_URL is not set as an environment variable!");
    let bucket_name: String = env::var("BUCKET_NAME").expect("BUCKET_NAME is not set as an environment variable!");
    let sqs_queue_name: String = env::var("SQS_QUEUE_NAME").expect("SQS_QUEUE_NAME is not set as an environment variable!");
    let log_level: String = env::var("RUST_LOG").expect("RUST_LOG is not set as an environment variable!");
    let namespace: String = env::var("NAMESPACE").expect("NAMESPACE is not set as an environment variable!");

    let queue_name = namespace.clone() + START_POSTFIX;

    info!("Using uri: {} with queue: {}", &broker_uri, &queue_name);
    info!("Configured log level: {}", &log_level);

    let queue_manager: QueueManager = create_queue_manager(broker_uri.as_str());
    create_namespaced_queues(&queue_manager, &namespace).await
        .expect("Could not create queues!");

    let _sqs_join_handle = setup_queue_listeners(bucket_name.as_str(), sqs_queue_name.as_str(), queue_manager.clone()).await;
    let _jobs_join_handle = setup_job_listeners(namespace.as_str(), queue_manager.clone()).await;

    queue_manager.start_listening(queue_name.as_str(), execute_model_job).await
            .expect("Could not start listener for queue!");

    Ok(())
}