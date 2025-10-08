import aws_cdk as cdk
from aws_cdk import aws_lambda as aws_lambda
import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import aws_sqs as sqs

from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_iam
from constructs import Construct

EXCLUDE_FILES = [
    "cdk.out",
    ".venv",
    ".ruff_cache",
    ".git",
    "__pycache__",
    "*.pyc",
    ".pytest_cache",
    ".idea",
    "data",
    "scripts",
    ".github",
    ".circleci",
]


class LgsfStack(cdk.Stack):
    """Main LGSF CDK stack with GitHub backend integration."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.dc_environment = self.node.try_get_context("dc-environment")
        self._stack_name = construct_id

        # Common naming prefix
        self.prefix = f"lgsf-{self.dc_environment}"

        # Create resources
        self.create_dependencies_layer()
        self.create_lambda_execution_role()
        self.create_sqs_queue()

        self.create_lambda_functions()
        self.create_event_rules()

        # Create outputs
        self.create_outputs()

    def get_resource_name(self, resource_type: str, resource_name: str) -> str:
        """Generate a consistent resource name."""
        return f"{self.prefix}-{resource_type}-{resource_name}"

    def get_lambda_function_name(self, function_name: str) -> str:
        """Generate a consistent Lambda function name."""
        return self.get_resource_name("lambda", function_name)

    def get_queue_name(self, queue_name: str) -> str:
        """Generate a consistent SQS queue name."""
        return self.get_resource_name("queue", queue_name)

    def get_role_name(self, role_name: str) -> str:
        """Generate a consistent IAM role name."""
        return self.get_resource_name("role", role_name)

    def create_dependencies_layer(self) -> None:
        """Create Lambda layer for Python dependencies."""

        self.dependencies_layer = aws_lambda_python.PythonLayerVersion(
            self,
            "DependenciesLayer",
            entry="lambda-layers/DependenciesLayer",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_12],
            description="Dependencies layer for LGSF Lambda functions",
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

    def create_lambda_execution_role(self) -> None:
        """Create IAM role for Lambda function execution with necessary permissions."""
        self.lambda_execution_role = aws_iam.Role(
            self,
            "LGSFLambdaExecutionRole",
            role_name="LGSFLambdaExecutionRole",
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Execution role for LGSF Lambda functions",
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Add SQS permissions
        self.lambda_execution_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:SendMessage",
                    "sqs:GetQueueUrl",
                ],
                resources=["*"],
            )
        )

        # Add AWS Systems Manager permissions for GitHub credentials
        self.lambda_execution_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameter",
                    "ssm:GetParameters",
                    "ssm:GetParametersByPath",
                ],
                resources=[
                    f"arn:aws:ssm:*:*:parameter/lgsf/{self.dc_environment}/github/token",
                    f"arn:aws:ssm:*:*:parameter/lgsf/{self.dc_environment}/github/repository_url",
                ],
            )
        )

        # Add CloudWatch Logs permissions
        self.lambda_execution_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogStreams",
                    "logs:DescribeLogGroups",
                ],
                resources=["*"],
            )
        )

    def create_sqs_queue(self) -> None:
        """Create SQS queue for scraper tasks."""
        # Create dead letter queue first
        dlq = sqs.Queue(
            self,
            "ScraperQueueDLQ",
            queue_name="ScraperQueue-DLQ",
            retention_period=cdk.Duration.days(14),
        )

        self.scraper_queue = sqs.Queue(
            self,
            "ScraperQueue",
            queue_name="ScraperQueue",
            visibility_timeout=cdk.Duration.minutes(15),
            retention_period=cdk.Duration.days(14),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=dlq,
            ),
        )

    def create_lambda_functions(self) -> None:
        """Create Lambda functions for queue building and scraping."""

        # Queue Builder Function
        self.queue_builder_function = aws_lambda.Function(
            self,
            "QueueBuilderFunction",
            function_name=self.get_lambda_function_name("queue-builder"),
            code=aws_lambda.Code.from_asset(
                ".",
                exclude=EXCLUDE_FILES,
            ),
            handler="lgsf.aws_lambda.handlers.queue_builder_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=cdk.Duration.minutes(15),
            layers=[self.dependencies_layer],
            role=self.lambda_execution_role,
            environment={
                "PYTHONPATH": "/var/task:/opt/python",
                "DC_ENVIRONMENT": self.dc_environment,
                "GITHUB_TOKEN_SSM_PATH": f"/lgsf/{self.dc_environment}/github/token",
                "GITHUB_REPOSITORY_URL_SSM_PATH": f"/lgsf/{self.dc_environment}/github/repository_url",
                "LGSF_STORAGE_BACKEND": "github",
            },
            description="Send scraper tasks to SQS queue",
        )

        # Scraper Worker Function
        self.scraper_worker_function = aws_lambda.Function(
            self,
            "ScraperWorkerFunction",
            function_name=self.get_lambda_function_name("scraper-worker"),
            code=aws_lambda.Code.from_asset(
                ".",
                exclude=EXCLUDE_FILES,
            ),
            handler="lgsf.aws_lambda.handlers.scraper_worker_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=cdk.Duration.minutes(15),
            layers=[self.dependencies_layer],
            role=self.lambda_execution_role,
            environment={
                "PYTHONPATH": "/var/task:/opt/python",
                "DC_ENVIRONMENT": self.dc_environment,
                "GITHUB_TOKEN_SSM_PATH": f"/lgsf/{self.dc_environment}/github/token",
                "GITHUB_REPOSITORY_URL_SSM_PATH": f"/lgsf/{self.dc_environment}/github/repository_url",
                "LGSF_STORAGE_BACKEND": "github",
            },
            description="Process scraper tasks from SQS queue",
        )

        # Add SQS event source to scraper worker
        self.scraper_worker_function.add_event_source(
            lambda_event_sources.SqsEventSource(
                self.scraper_queue,
                batch_size=1,
                enabled=True,
            )
        )

        # Grant permissions for queue operations
        self.scraper_queue.grant_send_messages(self.queue_builder_function)
        self.scraper_queue.grant_consume_messages(self.scraper_worker_function)

    def create_event_rules(self) -> None:
        """Create EventBridge rules for scheduled execution."""

        # Daily schedule rule for queue builder
        self.queue_builder_rule = events.Rule(
            self,
            "QueueBuilderScheduleRule",
            rule_name=self.get_resource_name("rule", "queue-builder"),
            description="Send scraper tasks to SQS",
            enabled=True,
            schedule=events.Schedule.rate(cdk.Duration.days(1)),
        )

        # Add Lambda target to the rule
        self.queue_builder_rule.add_target(
            targets.LambdaFunction(self.queue_builder_function)
        )

        # Grant EventBridge permission to invoke the Lambda
        self.queue_builder_function.add_permission(
            "AllowEventBridgeInvoke",
            principal=aws_iam.ServicePrincipal("events.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=self.queue_builder_rule.rule_arn,
        )

    def create_outputs(self) -> None:
        """Create CloudFormation outputs."""

        # Queue Builder Function ARN
        cdk.CfnOutput(
            self,
            "QueueBuilderFunctionArn",
            value=self.queue_builder_function.function_arn,
            description="Queue Builder Lambda Function ARN",
            export_name=f"{self._stack_name}-QueueBuilderFunctionArn",
        )

        # Scraper Worker Function ARN
        cdk.CfnOutput(
            self,
            "ScraperWorkerFunctionArn",
            value=self.scraper_worker_function.function_arn,
            description="Scraper Worker Lambda Function ARN",
            export_name=f"{self._stack_name}-ScraperWorkerFunctionArn",
        )

        # SQS Queue URL
        cdk.CfnOutput(
            self,
            "ScraperQueueUrl",
            value=self.scraper_queue.queue_url,
            description="SQS Queue URL for scraper tasks",
            export_name=f"{self._stack_name}-ScraperQueueUrl",
        )

        # Lambda Execution Role ARN
        cdk.CfnOutput(
            self,
            "LambdaExecutionRoleArn",
            value=self.lambda_execution_role.role_arn,
            description="Lambda Execution Role ARN",
            export_name=f"{self._stack_name}-LambdaExecutionRoleArn",
        )

        # GitHub Configuration Information
        cdk.CfnOutput(
            self,
            "GitHubConfigInfo",
            value=f"Store GitHub token at: /lgsf/{self.dc_environment}/github/token and repository URL at: /lgsf/{self.dc_environment}/github/repository_url",
            description="Information about GitHub configuration requirements",
        )
