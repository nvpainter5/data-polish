# AWS Deployment Guide

Deploy DataPolish as an AWS Lambda function triggered by S3 uploads.
Free-tier: well under monthly limits for portfolio-scale usage.

## Architecture

```
You upload a CSV
       |
       v
s3://datapolish-raw-<account>/incoming.csv
       |
       v   (S3 event)
DataPolish Lambda (container)
       |
       v
s3://datapolish-cleaned-<account>/cleaned/incoming_cleaned.parquet
                                  /cleaned/incoming_audit.json
```

The Lambda runs the Phase 1 pipeline: profile → propose → apply → validate
→ upload. ~30–60 seconds per invocation, 1024 MB memory.

## Prerequisites

You'll need installed locally:

- **AWS CLI v2** — [install instructions](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- **AWS SAM CLI** — [install instructions](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- **Docker Desktop** — [download](https://www.docker.com/products/docker-desktop/) (Lambda image is built locally then pushed to ECR by SAM)
- An **AWS account** with a free-tier-eligible IAM user

Verify with:

```bash
aws --version
sam --version
docker --version
```

## One-time AWS setup

### 1. Configure AWS credentials

If you haven't already, create an IAM user in the AWS console with programmatic access (Access key + Secret), then:

```bash
aws configure
```

Paste the access key, secret, default region (`us-east-1` is fine), and output format (`json`).

### 2. Confirm your identity

```bash
aws sts get-caller-identity
```

Should print your account ID and IAM ARN.

## Deploy

From the project root:

```bash
cd ~/DataPolish

# Build the Lambda container image (uses lambda/Dockerfile, project root as context)
sam build

# First deploy — guided, asks you a few questions interactively
sam deploy --guided
```

When `sam deploy --guided` prompts you:

- **Stack Name**: `datapolish`
- **AWS Region**: `us-east-1`
- **Parameter GroqApiKey**: paste your `gsk_...` key
- **Parameter ResourcePrefix**: leave as `datapolish`
- **Confirm changes before deploy**: `Y`
- **Allow SAM CLI IAM role creation**: `Y`
- **Disable rollback**: `N`
- **CleanerFunction may not have authorization defined**: `Y` (S3 trigger, no API auth needed)
- **Save arguments to configuration file**: `Y` (lets you redeploy with just `sam deploy` next time)

The first deployment takes 3–5 minutes — it creates an ECR repository, pushes the image, and creates the Lambda + S3 buckets + IAM role + S3 event trigger.

When it finishes, you'll see Outputs:

```
RawBucketName       =  datapolish-raw-123456789012
CleanedBucketName   =  datapolish-cleaned-123456789012
CleanerFunctionName =  datapolish-cleaner
CleanerFunctionArn  =  arn:aws:lambda:us-east-1:123456789012:function:datapolish-cleaner
```

## Try it end-to-end

```bash
# Upload a CSV (use the existing sample)
aws s3 cp data/raw/nyc_311_sample.csv s3://datapolish-raw-<account>/incoming.csv

# Tail Lambda logs as the function runs
sam logs --stack-name datapolish --tail
```

Wait ~30–60 seconds. Logs will show the pipeline stages. When done:

```bash
# List what landed in the cleaned bucket
aws s3 ls s3://datapolish-cleaned-<account>/cleaned/

# Pull down the audit JSON to inspect
aws s3 cp s3://datapolish-cleaned-<account>/cleaned/incoming_audit.json /tmp/
cat /tmp/incoming_audit.json | head -40
```

## Costs

All within free tier for portfolio-scale usage:

- **Lambda**: 1M requests/month free, 400,000 GB-seconds compute free. One pipeline run is ~30 seconds × 1 GB = 30 GB-seconds. You'd need ~13,000 runs/month to exceed the free tier.
- **S3**: 5 GB storage free, 20K GET/2K PUT free. Our outputs are ~5 MB each.
- **ECR**: 500 MB free private storage. Our image is ~400 MB.
- **CloudWatch Logs**: 5 GB free. Default 30-day retention.
- **Groq**: free tier (no AWS cost).

## Updating

After code changes:

```bash
sam build
sam deploy   # uses saved config, no prompts
```

## Tearing down

```bash
# Empty the S3 buckets (CloudFormation refuses to delete non-empty buckets)
aws s3 rm s3://datapolish-raw-<account>/ --recursive
aws s3 rm s3://datapolish-cleaned-<account>/ --recursive

# Delete the stack (and everything in it)
sam delete --stack-name datapolish
```

## Troubleshooting

- **"Image manifest is invalid" during sam build**: make sure Docker Desktop is running.
- **Lambda timeouts**: bump `Timeout` in `template.yaml` from 300 to 600 if you're processing very large CSVs.
- **413 from Groq**: same fix as in local dev — the cleaning module already slims the profile, so this should be rare. If it happens, check CloudWatch logs.
- **Hitting Groq rate limits during testing**: add a small delay between uploads, or set up Groq billing for higher TPM.

## Production hardening (post-portfolio)

Things you'd add for a real production deployment, listed for awareness:

- Replace the `GroqApiKey` parameter with AWS Secrets Manager (`Resolve` reference instead of plain env var).
- Add S3 server-side encryption (SSE-S3 or SSE-KMS).
- Add a dead-letter queue (SQS DLQ) for failed Lambda invocations.
- Add CloudWatch alarms on error rate / duration.
- Move logs through a structured-logging library (e.g., `aws-lambda-powertools`).
- Versioned Lambda + alias for blue/green updates.
