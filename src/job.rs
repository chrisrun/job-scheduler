use futures::{StreamExt, Stream, TryStreamExt, stream};
use k8s_openapi::api::batch::v1beta1::{CronJob};
use k8s_openapi::api::batch::v1::{JobStatus, Job, JobSpec};
use k8s_openapi::api::core::v1::{PodTemplateSpec, EnvVar};
use tracing::*;
use chrono::prelude::*;
use k8s_openapi::{
    apimachinery::pkg::apis::meta::v1::Time,
    chrono::{Duration, Utc},
};
//use kube_client::error::Error;
use dotenv::dotenv;
use std::collections::HashMap;
use regex::Regex;
use anyhow::{Result, Context};
use kube::{
    api::{Api, DeleteParams, ListParams, PostParams, ResourceExt, WatchEvent},
    runtime::{watcher, WatchStreamExt},
    Client,
};
use std::collections::HashSet;
use futures::stream::{BoxStream, SelectAll};


const DATE_FORMAT: &str = "%Y%m%d%H%M%S";
#[allow(clippy::large_enum_variant)]
pub enum Watched {
    Job(Job)
}

pub async fn run_job(model: &str, namespace: &str, input_location: &str, cleanup_min: usize, env_vars_map: &HashMap<String, String>) -> anyhow::Result<()> {
    info!("Running job for model: {:?} in namespace: {:?}", model, namespace);
    let mut env_vars: Vec<EnvVar> = env_vars_map.iter()
        .map(|(k, v)| EnvVar{name: k.to_string(), value: Some(v.to_string()), value_from: None})
        .collect();

    env_vars.push(EnvVar{name: "INPUT_LOCATION".to_string().to_uppercase(), value: Some(input_location.to_string()), value_from: None});

    let new_job = get_job_from_cronjob(model, namespace, env_vars).await?;
    execute_and_wait(new_job, namespace).await?;
    cleanup(&*(model.to_owned() + "-"), namespace, cleanup_min).await
}

pub async fn get_job_streams<'a>(namespace: &str) -> SelectAll<BoxStream<'a, Result<Watched, watcher::Error>>> {
    let client = Client::try_default().await.unwrap();

    let jobs: Api<Job> = Api::namespaced(client, namespace);
    let modification_watcher = watcher(jobs.clone(), ListParams::default());

    // select on applied events from all watchers
    let combo_stream = stream::select_all(vec![
        //creation_watcher.applied_objects().map_ok(Watched::Job).boxed(),
        modification_watcher.touched_objects().map_ok(Watched::Job).boxed(),
    ]);
    // SelectAll Stream elements must have the same Item, so all packed in this:
    combo_stream
}

pub async fn get_job_from_cronjob(source_job: &str, namespace: &str, env_vars: Vec<EnvVar>) -> Result<Job, anyhow::Error> {
    let client = Client::try_default().await?;
    let jobs: Api<CronJob> = Api::namespaced(client, namespace);

    info!("Getting the job record for job: {:?} in namespace: {:?}.", source_job, namespace);
    let job: CronJob = jobs.get(source_job).await?;
    let utc: DateTime<Utc> = Utc::now();
    let destination_job = source_job.to_owned() + "-" + &utc.format(DATE_FORMAT).to_string();

    let mut job_metadata = job.metadata.clone();
    job_metadata.name = Some(destination_job.to_string());
    job_metadata.labels = None;
    job_metadata.managed_fields = None;
    job_metadata.resource_version = None;

    let mut job_spec: JobSpec = job.spec
        .with_context(|| "Could not get job spec!")?.job_template.spec
        .with_context(|| "Could not get job spec!")?.clone();
    let mut pod_template_spec: PodTemplateSpec = job_spec.template;

    let mut pod_spec = pod_template_spec.spec.with_context(|| "Could not clone job spec!")?;
    let mut metadata = pod_template_spec.metadata.with_context(|| "Could not clone job metadata!")?;
    metadata.labels = None;

    let mut new_env_list: Vec<EnvVar> = vec![];

    let reserved_vars: HashSet<String> = env_vars.clone()
        .iter().map(|var| var.name.clone() ).collect();

    let mut container = pod_spec.containers.last_mut().with_context(|| "Could not get job container!")?.clone();
    let orig_envs: Option<Vec<EnvVar>> = container.env;

    if orig_envs.is_some() {
        for item in orig_envs.unwrap() {
            if !reserved_vars.contains(&item.name) {
                debug!("adding variable from orig_envs: {:?}", item.clone());
                new_env_list.push(item.clone());
            }
        }
    }

    for env_var in env_vars.clone() {
        debug!("adding variable from env_vars {:?}", env_var.clone());
        new_env_list.push(env_var.clone());
    }

    container.env = Some(new_env_list);

    pod_spec.containers = vec![];
    pod_spec.containers.push(container.clone());

    pod_template_spec.spec = Some(pod_spec);
    pod_template_spec.metadata = Some(metadata);
    job_spec.template = pod_template_spec;
    job_spec.selector = None;
    job_spec.completions = Some(1);
    job_spec.parallelism = Some(1);

    info!("Returning new job object from cronjob: {:?} with args: {:?}", destination_job, &env_vars);
    let empty_status = JobStatus{ active: None, completion_time: None, conditions: None, failed: None,
                                    start_time: None, succeeded: None};
    let batch_job = Job { metadata: job_metadata, spec: Some(job_spec), status: Some(empty_status) };

    Ok(batch_job)
}


pub async fn get_copy_of(source_job: &str, namespace: &str, env_vars: Vec<EnvVar>) -> Result<Job, anyhow::Error> {
    let client = Client::try_default().await?;
    let jobs: Api<Job> = Api::namespaced(client, namespace);

    info!("Getting the job record for job: {:?}.", source_job);
    let mut job: Job = jobs.get(source_job).await?;
    let utc: DateTime<Utc> = Utc::now();
    let destination_job = source_job.to_owned() + "-" + &utc.format(DATE_FORMAT).to_string();

    let mut job_metadata = job.metadata.clone();
    job_metadata.name = Some(destination_job.to_string());
    job_metadata.labels = None;
    job_metadata.managed_fields = None;
    job_metadata.resource_version = None;

    let mut job_spec: JobSpec = job.spec.clone().with_context(|| "Could not clone job spec!")?;
    let mut pod_template_spec: PodTemplateSpec = job_spec.template;

    let mut pod_spec = pod_template_spec.spec.with_context(|| "Could not retrieve template job spec!")?;
    let mut metadata = pod_template_spec.metadata.with_context(|| "Could not retrieve template job metadata!")?;
    metadata.labels = None;

    let mut new_env_list: Vec<EnvVar> = vec![];
    for env_var in env_vars.clone() {
        new_env_list.push(env_var.clone());
    }

    let mut container = pod_spec.containers.last_mut().with_context(|| "Could not retrieve job Container!")?.clone();
    container.env = Some(new_env_list);

    pod_spec.containers = vec![];
    //pod_spec_metadata.labels = None;
    pod_spec.containers.push(container.clone());

    pod_template_spec.spec = Some(pod_spec);
    pod_template_spec.metadata = Some(metadata);
    job_spec.template = pod_template_spec;
    job_spec.selector = None;
    job_spec.completions = Some(1);
    job_spec.parallelism = Some(1);
    //overwriting job object
    job.spec = Some(job_spec);
    job.metadata = job_metadata;

    debug!("Created Job Object: {:?}", &job);
    info!("Returning copy of new job object: {:?} with args: {:?}", destination_job, &env_vars);
    return Ok(job);
}


pub async fn execute<'a>(job: Job, namespace: &str) -> impl Stream<Item = Result<WatchEvent<Job>, kube::Error>> {
    let client = Client::try_default().await.unwrap();

    let jobs: Api<Job> = Api::namespaced(client, namespace);
    let pp = PostParams::default();

    jobs.create(&pp, &job).await.unwrap();

    // See if it ran to completion
    let lp = ListParams::default()
        .fields(&format!("metadata.name={}", &job.name())) // only want events for our job
        .timeout(60);

    let stream = jobs.watch(&lp, "").await.unwrap();
    stream
}

pub async fn execute_and_wait(job: Job, namespace: &str) -> anyhow::Result<()> {
    let mut stream = execute(job, namespace).await.boxed();

    while let Some(status) = stream.try_next().await? {
        match status {
            WatchEvent::Added(s) => {
                info!("Added {:?}", &s)
            }
            WatchEvent::Modified(s) => {
                info!("Received status: {:?}", &s);

                let current_status = s.status.clone().expect("Status is missing");
                let status_json = serde_json::to_string(&current_status).unwrap();
                info!("Received status: {:?}", &status_json);
                match current_status.completion_time {
                    Some(_) => {
                        info!("Modified: {} is complete", s.name());
                        break;
                    }
                    _ => info!("Modified: {} is running", s.name()),
                }
            }
            WatchEvent::Deleted(s) => info!("Deleted {}", s.name()),
            WatchEvent::Error(s) => error!("XXA : {:?}", s),
            _ => {}
        }
    }
    Ok(())
}

pub async fn cleanup(job_name_prefix: &str, namespace: &str, min_jobs: usize) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let jobs: Api<Job> = Api::namespaced(client, namespace);
    let lp = ListParams::default();
    info!("Cleaning up job records in the namespace {}, with job name prefixes: {}", namespace, job_name_prefix);
    //list all jobs
    let job_list = jobs.list(&lp).await?;

    let job_map: HashMap<_, _> = job_list.iter()
        .map(|job| job.clone().name())
        .filter(|name| name.clone().starts_with(job_name_prefix))
        .map(|name| (name.clone(), name.clone())).collect();

    let mut sorted: Vec<_> = job_map.iter().collect();
    info!("Sorted job list: {:?}", sorted);

    // sort Vector by key from HashMap. Each tuple is sorted by its first item.
    sorted.sort_by_key(|a| a.0);
    sorted.reverse();

    let regex = Regex::new(r".+-")?;
    let count = sorted.iter().count();
    info!("Found {} Jobs with prefix: {}", count, job_name_prefix);
    if count > min_jobs {
        for (key, value) in sorted[min_jobs..].iter() {
            let date_string = regex.replace_all(key, "");
            info!("Removing job with date string: {:?}", date_string);
            delete_job(value, namespace).await?;
        }
    } else {
        info!("Count: {} is lower or equals than min jobs: {} - Nothing to delete!", count, min_jobs)
    }

    Ok(())
}

pub async fn delete_job(job_removal: &str, namespace: &str) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let jobs: Api<Job> = Api::namespaced(client, namespace);
    info!("Deleting job {} from namespace: {}...", job_removal, namespace);
    jobs.delete(job_removal, &DeleteParams::background().dry_run())
        .await?;
    jobs.delete(job_removal, &DeleteParams::background()).await?;
    info!("Finished deletion");
    Ok(())
}

pub fn format_creation_since(time: Time) -> String {
    format_duration(Utc::now().signed_duration_since(time.0))
}

pub fn format_duration(dur: Duration) -> String {
    match (dur.num_days(), dur.num_hours(), dur.num_minutes()) {
        (days, _, _) if days > 0 => format!("{}d", days),
        (_, hours, _) if hours > 0 => format!("{}h", hours),
        (_, _, mins) => format!("{}m", mins),
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    dotenv().ok();

    let env_var1 = EnvVar{name: "BUCKET_NAME".to_string().to_uppercase(), value: Some("test-dev-new".to_string()), value_from: None};
    let env_var2 = EnvVar{name: "OUTPUT_LOCATION".to_string().to_uppercase(), value: Some("test-dev-new/clustering-model/grouped_results.csv".to_string()), value_from: None};
    let env_var3 = EnvVar{name: "OUTPUT_LOCATION2".to_string().to_uppercase(), value: Some("something new".to_string()), value_from: None};

    let new_job = get_job_from_cronjob("clustering-model", "default", vec![env_var1, env_var2, env_var3]).await;
    info!("new job output: {:?}", new_job);
    
    Ok(())
}
