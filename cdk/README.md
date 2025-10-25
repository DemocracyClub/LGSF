# LGSF CDK Deployment

This directory contains the AWS CDK (Cloud Development Kit) v2 deployment code for the Local Government Scraper Framework (LGSF), migrated from AWS SAM.

## Overview

The CDK deployment creates the following AWS resources:

- **Lambda Functions**: 
  - Queue Builder Function (scheduled daily)
  - Scraper Worker Function (triggered by SQS)
- **SQS Queue**: For distributing scraper tasks
- **Lambda Layer**: For Python dependencies
- **IAM Roles**: With necessary permissions for Lambda execution
- **EventBridge Rule**: For daily scheduling

## Architecture

```
EventBridge (daily) -> Queue Builder Lambda -> SQS Queue -> Scraper Worker Lambda -> GitHub/Local Storage
```

## Prerequisites

1. **Python Dependencies**: Install CDK dependencies
   ```bash
   uv sync --group cdk
   ```

2. **Node.js Dependencies**: Install CDK CLI
   ```bash
   npm install
   ```

3. **AWS CLI**: Configured with appropriate credentials
   ```bash
   aws configure --profile lgsf-dev-dc
   ```

## Deployment Commands

### Using the Makefile (Recommended)

```bash
# Install NPM dependencies
make npm-install

# Bootstrap CDK (first time only)
make cdk-bootstrap

# Show what will be deployed
make cdk-synth

# Show differences from current deployment
make cdk-diff

# Deploy the stack
make cdk-deploy

# Destroy the stack
make cdk-destroy
```

### Using the Deployment Script

```bash
# Deploy with default settings
./scripts/deploy-cdk.sh

# Deploy to different environment
./scripts/deploy-cdk.sh -e production -p lgsf-prod-dc

# Bootstrap CDK
./scripts/deploy-cdk.sh -a bootstrap

# Show differences
./scripts/deploy-cdk.sh -a diff

# Destroy stack
./scripts/deploy-cdk.sh -a destroy
```

### Using CDK CLI Directly

```bash
# Set environment variables
export AWS_PROFILE=lgsf-dev-dc
export DC_ENVIRONMENT=development

# Bootstrap (first time only)
npx cdk bootstrap

# Deploy
npx cdk deploy

# Destroy
npx cdk destroy
```

## Environment Configuration

The deployment supports multiple environments through the `DC_ENVIRONMENT` variable:

- `development` (default): Uses `lgsf-dev-dc` AWS profile
- `production`: Uses `lgsf-prod-dc` AWS profile

## File Structure

```
cdk/
├── README.md                     # This file
├── app.py                        # CDK app entry point
└── stacks/
    ├── __init__.py
    └── lgsf_stack.py            # Single consolidated LGSF stack
```

## Migration from SAM

This CDK deployment replaces the original SAM template (`sam-template.yaml`) with the following mappings:

| SAM Resource | CDK Resource | Notes |
|--------------|--------------|-------|
| `DependenciesLayer` | `DependenciesLayer` | Lambda layer for Python dependencies |
| `QueueBuilderFunction` | `QueueBuilderFunction` | Scheduled Lambda function |
| `SQSScraperQueue` | `ScraperQueue` | SQS queue with DLQ |
| `ScraperWorkerFunction` | `ScraperWorkerFunction` | SQS-triggered Lambda function |
| Schedule Event | EventBridge Rule | Daily scheduling |

## Key Improvements

1. **Type Safety**: Python CDK provides better type checking
2. **Simplified Structure**: Single stack file with all resources
3. **Environment Management**: Better support for multiple environments
4. **Error Handling**: Added dead letter queue for failed messages
5. **Resource Naming**: Consistent naming conventions
6. **Permissions**: More granular IAM permissions

## Troubleshooting

### Common Issues

1. **CDK Bootstrap Required**
   ```
   Error: Need to perform AWS CDK bootstrap
   Solution: Run `make cdk-bootstrap`
   ```

2. **Lambda Layer Build Fails**
   ```
   Error: Requirements file not found
   Solution: Run `make lambda-layers/DependenciesLayer/requirements.txt`
   ```

3. **AWS Credentials**
   ```
   Error: Unable to locate credentials
   Solution: Check AWS profile configuration
   ```

### Debugging

- View synthesized CloudFormation: `make cdk-synth`
- Check differences: `make cdk-diff`
- Watch for changes: `make cdk-watch`

## Stack Outputs

After deployment, the following outputs are available:

- `QueueBuilderFunctionArn`: ARN of the queue builder Lambda
- `ScraperWorkerFunctionArn`: ARN of the scraper worker Lambda  
- `ScraperQueueUrl`: URL of the SQS queue
- `LambdaExecutionRoleArn`: ARN of the Lambda execution role

## Security

- Lambda functions use least-privilege IAM roles
- SQS queue has a dead letter queue for error handling
- All resources are tagged for cost tracking
- No hardcoded secrets or credentials

## Cost Optimization

- Lambda functions have appropriate timeout settings
- SQS messages have retention periods to avoid storage costs
- Resources are tagged for cost allocation
- Dead letter queue prevents infinite retries

## Support

For issues with the CDK deployment, please check:

1. AWS CloudFormation console for detailed error messages
2. CloudWatch Logs for Lambda function execution logs
3. CDK documentation at https://docs.aws.amazon.com/cdk/