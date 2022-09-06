# Kubernetes Job Scheduler

Kubernetes job scheduler, coordinates messages from rabbitmq and AWS SQS and launches kubernetes jobs accordingly. Apart of that it will listen as well on S3 bucket events and launch a job if the state of a S3 bucket changes.  


## Introduction

The kubernetes job scheduler will listen to file movements of a S3 bucket, as well as rabbit mq messages. It will execute jobs on file arrival to S3, it will get the job it needs to execute from the "job-name", which will be a part of the S3 input path: "environment/job-name/input.csv". This "job-name" will exeucte a batch job with the same name, that exists in the namespace on the cluster.

Each execution will send out the following status messages to the rabbit mq receiver:

```
	name: "job-name",
    input_location: "environment/job-name/input.csv", #input location on the s3 bucket
    action: "running", #the current state of the job
    status: #JobStatus representing the current state of a Job
		active: 1 #The number of actively running pods
		completionTime: 2019-10-12T07:20:50.52Z #Represents time when the job was completed. It is not guaranteed to be set in happens-before order across separate operations
		startTime: 2019-10-12T07:15:43.39Z #Represents time when the job was acknowledged by the job controller.
		failed: 0 #The number of pods which reached phase Failed
		succeeded 1 #The number of pods which reached phase Succeeded
    environment: test, 
    error:
		message: #a message about the error
    	reason: #the reason for the error
		code: #kubernetes provided error code
```


## Configuration

| Name                      | Description                                     | Value |
| ------------------------- | ----------------------------------------------- | ----- |
| `BROKER_URL` |  | `amqp://guest:guest@127.0.0.1:5672//` |
| `BUCKET_NAME` |  | `local-tests`, the bucket will be created if it does not exist. |
| `SQS_QUEUE_NAME` | Name of the AWS simple queue, if it does not exist, it will be created and configured to listen to events on the configured bucket. | `local-tests` |
| `RUST_LOG` | Log level for rust log | `debug` |
| `NAMESPACE` | The namespace that will be used to  | `testns` |
| `CLEANUP_MIN` | The minimum number of jobs that should be kept by the scheduler, older exeuctions will be cleaned up. | `3` |


## Building

```
	cargo build 
```
