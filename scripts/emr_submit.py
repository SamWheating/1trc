"""
Script for defining, submitting and monitoring an EMR job flow, for reproducibility.

Requires:
 - a working python installation (>=3.6) with boto3
 - a local AWS role called `default` with access to create and watch EMR resources
 - an S3 bucket with the 1trc JAR present
"""

import logging

import boto3

# This is the bucket where the JAR is loaded from, the output files and logs are written to
S3_BUCKET_BASE = "s3://samwheating-1trc"

# Smaller subsets of the data to save some $$$ on test runs
SMALL_INPUT = "s3://coiled-datasets-rp/1trc/measurements-1000*.parquet"  # 11 files, ~263 MB
MED_INPUT = "s3://coiled-datasets-rp/1trc/measurements-1*.parquet"  # 11111 files, 260.1GB
FULL_INPUT = "s3://coiled-datasets-rp/1trc/*.parquet"  # 10k files, ~2.4 TB

JOB_SPEC = {
    "Name": "1 Trillion Row Challenge",
    "ReleaseLabel": "emr-6.15.0", # latest 6.x release
    "Instances": {
        "Placement": {
            "AvailabilityZone": "us-east-1a", # same region as the source data
        },
        "InstanceGroups": [
            {
                "Name": "Coordinator",
                "Market": "ON_DEMAND", # Use on-demand here since coordinator interruptions are costly.
                "InstanceRole": "MASTER",
                "InstanceType": "m6i.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Workers",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m6i.xlarge",  # ~$0.077/hr with high network bandwidth
                "InstanceCount": 31, # since I only have quota for 128 spot vCPUs :(
            },
        ],
    },
    "LogUri": f"{S3_BUCKET_BASE}/logs/",
    "Steps": [
        {
            "Name": "Spark Submit Step",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    f"{S3_BUCKET_BASE}/jars/1trc.jar",
                    FULL_INPUT,  # job input arg [1]: input file prefix
                    f"{S3_BUCKET_BASE}/output/",  # job input arg [2]: output file prefix
                ],
            },
        },
    ],
    "JobFlowRole": "EMRInstanceRole",
    "ServiceRole": "EMRServiceRole",
    "Applications": [
        {"Name": "spark"},
    ],
    "Configurations": [
        {
            "Classification": "emrfs-site", # this is required in order to use a requester-pays bucket.
            "Properties": {"fs.s3.useRequesterPaysHeader": "true"},
        },
    ],
}

if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    session = boto3.Session(profile_name="default")
    client = boto3.client("emr")

    resp = client.run_job_flow(**JOB_SPEC)

    jobFlowId = resp["JobFlowId"]

    logging.info("Submitted job %s", jobFlowId)
