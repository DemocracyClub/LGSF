import aws_cdk as cdk
import aws_cdk.aws_lambda_python_alpha as aws_lambda_python
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam, aws_logs
from aws_cdk import aws_lambda as aws_lambda
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
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
    """Main LGSF CDK stack with Step Functions orchestration."""

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

        # Fetch GitHub credentials from Parameter Store at build time
        self.github_token = ssm.StringParameter.value_for_string_parameter(
            self, f"/lgsf/{self.dc_environment}/github/token"
        )
        self.github_organization = ssm.StringParameter.value_for_string_parameter(
            self, f"/lgsf/{self.dc_environment}/github/organization"
        )

        # Create resources
        self.create_dependencies_layer()
        self.create_lambda_execution_role()
        self.create_lambda_functions()
        self.create_step_function()
        self.create_event_rules()

        # Create outputs
        self.create_outputs()

    def get_resource_name(self, resource_type: str, resource_name: str) -> str:
        """Generate a consistent resource name."""
        return f"{self.prefix}-{resource_type}-{resource_name}"

    def get_lambda_function_name(self, function_name: str) -> str:
        """Generate a consistent Lambda function name."""
        return self.get_resource_name("lambda", function_name)

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

        # SSM permissions removed - GitHub credentials are now passed as environment variables

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

        # Add Step Functions permissions
        self.lambda_execution_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "states:StartExecution",
                    "states:DescribeExecution",
                    "states:StopExecution",
                ],
                resources=["*"],
            )
        )

    def create_lambda_functions(self) -> None:
        """Create Lambda functions for Step Functions orchestration."""

        # Council Enumerator Function - discovers all councils to scrape
        self.council_enumerator_function = aws_lambda.Function(
            self,
            "CouncilEnumeratorFunction",
            function_name=self.get_lambda_function_name("council-enumerator"),
            code=aws_lambda.Code.from_asset(
                ".",
                exclude=EXCLUDE_FILES,
            ),
            handler="lgsf.aws_lambda.handlers.council_enumerator_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=cdk.Duration.minutes(5),
            layers=[self.dependencies_layer],
            role=self.lambda_execution_role,
            environment={
                "PYTHONPATH": "/var/task:/opt/python",
                "DC_ENVIRONMENT": self.dc_environment,
                "GITHUB_TOKEN": self.github_token,
                "GITHUB_ORGANIZATION": self.github_organization,
                "LGSF_STORAGE_BACKEND": "github",
            },
            description="Enumerate councils for parallel scraping",
        )

        # Scraper Worker Function - processes individual council scrapers
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
            reserved_concurrent_executions=5,  # Reserve capacity to prevent TooManyRequests
            environment={
                "PYTHONPATH": "/var/task:/opt/python",
                "DC_ENVIRONMENT": self.dc_environment,
                "GITHUB_TOKEN": self.github_token,
                "GITHUB_ORGANIZATION": self.github_organization,
                "LGSF_STORAGE_BACKEND": "github",
            },
            description="Process individual council scraper tasks",
        )

        # Post Processing Function - stub for now, runs after all scrapers complete
        self.post_processing_function = aws_lambda.Function(
            self,
            "PostProcessingFunction",
            function_name=self.get_lambda_function_name("post-processing"),
            code=aws_lambda.Code.from_asset(
                ".",
                exclude=EXCLUDE_FILES,
            ),
            handler="lgsf.aws_lambda.handlers.post_processing_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=cdk.Duration.minutes(5),
            layers=[self.dependencies_layer],
            role=self.lambda_execution_role,
            environment={
                "PYTHONPATH": "/var/task:/opt/python",
                "DC_ENVIRONMENT": self.dc_environment,
                "GITHUB_TOKEN": self.github_token,
                "GITHUB_ORGANIZATION": self.github_organization,
                "LGSF_STORAGE_BACKEND": "github",
            },
            description="Post-processing tasks after all scrapers complete",
        )

    def create_step_function(self) -> None:
        """Create Step Functions state machine for orchestrating scrapers."""

        # Step 1: Enumerate councils
        enumerate_councils_task = tasks.LambdaInvoke(
            self,
            "EnumerateCouncils",
            lambda_function=self.council_enumerator_function,
            output_path="$.Payload",
        )

        # Step 2: Fan-out to parallel scraper executions with distributed map
        parallel_scrapers_map = sfn.DistributedMap(
            self,
            "ParallelScrapers",
            items_path="$.councils",
            max_concurrency=5,  # Increased concurrency for distributed map
            result_path=sfn.JsonPath.DISCARD,  # Don't accumulate results to avoid payload size limits
        )

        # Individual scraper task with retry configuration
        scraper_task = (
            tasks.LambdaInvoke(
                self,
                "RunScraper",
                lambda_function=self.scraper_worker_function,
                payload=sfn.TaskInput.from_json_path_at("$"),
                retry_on_service_exceptions=False,
                result_path=sfn.JsonPath.DISCARD,  # Don't keep individual results
            )
            .add_retry(
                errors=["Lambda.TooManyRequestsException"],
                interval=cdk.Duration.seconds(20),
                max_attempts=10,
                backoff_rate=2.0,
            )
            .add_retry(
                errors=[
                    "States.TaskFailed",
                    "Lambda.ServiceException",
                    "Lambda.AWSLambdaException",
                ],
                interval=cdk.Duration.seconds(2),
                max_attempts=3,
                backoff_rate=2.0,
            )
            .add_catch(
                errors=["States.ALL"],
                handler=sfn.Pass(
                    self,
                    "HandleScraperError",
                    result=sfn.Result.from_object({"status": "failed"}),
                ),
                result_path="$.error",
            )
        )

        parallel_scrapers_map.item_processor(scraper_task)

        # Step 3: Post-processing after all scrapers complete
        post_processing_task = tasks.LambdaInvoke(
            self,
            "PostProcessing",
            lambda_function=self.post_processing_function,
        )

        # Chain the steps together
        definition = enumerate_councils_task.next(parallel_scrapers_map).next(
            post_processing_task
        )

        # Create CloudWatch log group for Step Functions logging
        step_function_log_group = aws_logs.LogGroup(
            self,
            "StepFunctionLogGroup",
            log_group_name=f"/aws/stepfunctions/{self.get_resource_name('stepfn', 'scraper-orchestration')}",
            retention=aws_logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # Create scraper-type specific log groups
        self.scraper_log_groups = {}
        # Current scraper types - add more as they're implemented
        scraper_types = [
            "councillors",
            "metadata",
            "templates",
            "meetings",
            "committees",
        ]

        for scraper_type in scraper_types:
            log_group = aws_logs.LogGroup(
                self,
                f"ScraperLogGroup{scraper_type.title()}",
                log_group_name=f"/aws/lambda/lgsf-scrapers/{scraper_type}",
                retention=aws_logs.RetentionDays.ONE_MONTH,
                removal_policy=cdk.RemovalPolicy.DESTROY,
                # Add tags for better resource management
                tags={
                    "Project": "LGSF",
                    "ScraperType": scraper_type,
                    "Purpose": "Scraper logging",
                },
            )
            self.scraper_log_groups[scraper_type] = log_group

        # Create IAM role for Step Functions
        self.step_function_role = aws_iam.Role(
            self,
            "LGSFStepFunctionRole",
            role_name=self.get_role_name("stepfn"),
            assumed_by=aws_iam.ServicePrincipal("states.amazonaws.com"),
            description="Execution role for LGSF Step Functions",
        )

        # Create the state machine
        self.step_function = sfn.StateMachine(
            self,
            "LGSFScraperOrchestration",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=self.step_function_role,  # Assign execution role
            timeout=cdk.Duration.hours(
                8
            ),  # Sufficient timeout for parallel processing with retries
            comment="LGSF scraper orchestration using Step Functions with distributed map",
            state_machine_type=sfn.StateMachineType.STANDARD,  # Required for distributed maps
            logs=sfn.LogOptions(
                destination=step_function_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
        )

        # Grant Step Functions permission to invoke Lambda functions
        self.council_enumerator_function.grant_invoke(self.step_function_role)
        self.scraper_worker_function.grant_invoke(self.step_function_role)
        self.post_processing_function.grant_invoke(self.step_function_role)

        # Add permissions for distributed map execution
        self.step_function_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "states:StartExecution",
                    "states:DescribeExecution",
                    "states:StopExecution",
                    "states:ListExecutions",
                    "states:DescribeStateMachine",
                    "states:DescribeStateMachineForExecution",
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                    "states:SendTaskHeartbeat",
                    "states:GetActivityTask",
                    "states:DescribeActivity",
                ],
                resources=["*"],
            )
        )

        # Add CloudWatch Logs permissions for distributed map
        self.step_function_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # Grant Lambda functions permission to write to scraper-specific log groups
        scraper_log_arns = [
            log_group.log_group_arn for log_group in self.scraper_log_groups.values()
        ]
        if scraper_log_arns:
            # Use a single policy statement for all log groups to avoid policy size limits
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
                    resources=[f"{log_arn}*" for log_arn in scraper_log_arns],
                )
            )

    def create_event_rules(self) -> None:
        """Create EventBridge rules for scheduled execution."""

        # Daily schedule rule for step function execution
        self.scraper_orchestration_rule = events.Rule(
            self,
            "ScraperOrchestrationScheduleRule",
            rule_name=self.get_resource_name("rule", "scraper-orchestration"),
            description="Trigger LGSF scraper orchestration via Step Functions",
            enabled=True,
            schedule=events.Schedule.rate(cdk.Duration.days(1)),
        )

        # Create IAM role for EventBridge rule
        eventbridge_role = aws_iam.Role(
            self,
            "EventBridgeExecutionRole",
            role_name=self.get_role_name("eventbridge"),
            assumed_by=aws_iam.ServicePrincipal("events.amazonaws.com"),
            description="Role for EventBridge to start Step Functions executions",
        )

        # Grant EventBridge permission to start Step Functions executions
        eventbridge_role.add_to_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=["states:StartExecution"],
                resources=[self.step_function.state_machine_arn],
            )
        )

        # Add Step Functions target to the rule with execution role
        self.scraper_orchestration_rule.add_target(
            targets.SfnStateMachine(self.step_function, role=eventbridge_role)
        )

    def create_outputs(self) -> None:
        """Create CloudFormation outputs."""

        # Step Function ARN
        cdk.CfnOutput(
            self,
            "ScraperOrchestrationStateMachineArn",
            value=self.step_function.state_machine_arn,
            description="LGSF Scraper Orchestration Step Function ARN",
            export_name=f"{self._stack_name}-ScraperOrchestrationStateMachineArn",
        )

        # Council Enumerator Function ARN
        cdk.CfnOutput(
            self,
            "CouncilEnumeratorFunctionArn",
            value=self.council_enumerator_function.function_arn,
            description="Council Enumerator Lambda Function ARN",
            export_name=f"{self._stack_name}-CouncilEnumeratorFunctionArn",
        )

        # Scraper Worker Function ARN
        cdk.CfnOutput(
            self,
            "ScraperWorkerFunctionArn",
            value=self.scraper_worker_function.function_arn,
            description="Scraper Worker Lambda Function ARN",
            export_name=f"{self._stack_name}-ScraperWorkerFunctionArn",
        )

        # Post Processing Function ARN
        cdk.CfnOutput(
            self,
            "PostProcessingFunctionArn",
            value=self.post_processing_function.function_arn,
            description="Post Processing Lambda Function ARN",
            export_name=f"{self._stack_name}-PostProcessingFunctionArn",
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
            value=f"Store GitHub token at: /lgsf/{self.dc_environment}/github/token and organization at: /lgsf/{self.dc_environment}/github/organization",
            description="Information about GitHub configuration requirements",
        )
