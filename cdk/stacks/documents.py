"""
The document pipeline on AWS (docs/document-pipeline.md): what turns the
documents the scrapers store into text, chunks and embeddings, beside
each document in the data bucket. One construct, added to the LGSF stack.

```
DataCommitsTopic (a push to LGSF-Data)   ─┐
Step Function finished (any end state)    ├─> trigger Lambda ─> documents queue
EventBridge, every hour                   ─┘      │
                                                  └─> Batch job, when the extract
                                                      queue has messages
documents queue ─> dispatch Lambda ─┬─> extract queue ─> Batch job (Docling,
                                    │                     Dockerfile.documents)
                                    │                       └─> embed queue
                                    └─> embed queue ─> embed Lambda (Bedrock)
                                                             └─> file-ready topic
```

The trigger finds files to look at and puts one message a file on the
documents queue. The dispatch function settles each without Docling
(lgsf.embeddings.pipeline.dispatch): a file that is restricted, empty or
not a document gets its status; one that only needs embedding goes to the
embed queue; one that needs converting goes to the extract queue. Docling
and torch are too large for the Lambda layer, so conversion runs on AWS
Batch: the trigger submits a job every hour while the extract queue has
messages, and the job drains it, sending each converted file on to the
embed queue. The embed function calls Bedrock and writes the embeddings.
Each function publishes to the file-ready topic when a file's status is
written, for anything that reads the results (reps-wiki's search, say).

Every queue has a dead-letter queue, with an alarm on it, so a file the
pipeline can't settle is seen rather than retried for ever.

The handlers are lgsf.embeddings.aws; the Batch job runs
`manage.py embeddings --drain-queue`.
"""

from dataclasses import dataclass

import aws_cdk as cdk
from aws_cdk import aws_batch as batch
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cloudwatch_actions
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr_assets as ecr_assets
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_iam, aws_logs
from aws_cdk import aws_lambda as aws_lambda
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

#: The embedding model, and the only one the embed function may call.
EMBEDDER = "titan-v2"
TITAN_V2_MODEL_ARN = (
    "arn:aws:bedrock:eu-west-2::foundation-model/amazon.titan-embed-text-v2:0"
)

#: Files stored before this instant are left to the backfill command
#: (manage.py embeddings --council …); the trigger and the sweep queue
#: only files stored after it. Held in SSM, /lgsf/<env>/documents/since,
#: which the stack creates with this value: to move it, change it here
#: and deploy, or the next deploy puts it back.
DOCUMENTS_SINCE = "2026-11-09T00:00:00Z"

#: The execution states the sweep runs after: every way a run can end.
STATE_MACHINE_END_STATES = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]

#: How many times a message is tried before it goes to the dead-letter
#: queue. Every queue keeps a message for SQS's longest, so a file waits
#: out a Spot capacity drought or a stopped Batch job rather than expiring.
MAX_RECEIVE_COUNT = 5
QUEUE_RETENTION = cdk.Duration.days(14)

#: The Batch worker extends a message's visibility while Docling converts
#: its file, so this only has to outlast the gap between extensions, with
#: room for a worker that dies mid-file.
EXTRACT_VISIBILITY_TIMEOUT = cdk.Duration.hours(2)

TRIGGER_TIMEOUT = cdk.Duration.minutes(15)
DISPATCH_TIMEOUT = cdk.Duration.minutes(5)
#: Dispatch functions at once: the event source keeps to it and the
#: reservation makes it a ceiling, as for embedding below.
DISPATCH_CONCURRENCY = 10
EMBED_TIMEOUT = cdk.Duration.minutes(15)

#: Titan's quota is 300,000 tokens a minute for the account. The embed
#: function reads a batch of up to ten files and embeds each file's
#: chunks from eight threads, so four of them at once is as much as the
#: quota comfortably takes; more would spend their time being throttled.
EMBED_CONCURRENCY = 4

#: An extract message older than this means the Batch job isn't keeping
#: up, or isn't running: the trigger submits one every hour while the
#: queue has messages.
EXTRACT_AGE_ALARM = cdk.Duration.hours(6)


@dataclass
class Instances:
    """The instances Batch jobs run on, and what each job asks for."""

    instance_types: list[str]
    image_type: batch.EcsMachineImageType
    hardware_type: ecs.AmiHardwareType
    #: The dependency group Dockerfile.documents installs.
    dependency_group: str
    vcpus: int
    #: Less than the instance's memory, which ECS keeps some of.
    memory_gib: int
    gpus: int
    #: The most vCPUs running at once, so the most jobs. Batch counts
    #: every vCPU of an instance.
    max_vcpus: int
    #: Docling processes a job runs, and threads for each.
    workers: int
    threads: int


INSTANCES = {
    # 16 vCPUs and 32 GiB or more. Docling on a CPU converts about a page
    # a second a worker; the hourly job drains what the day's scrapes
    # found. Ten kinds across the region's zones, so Spot has a pool to
    # choose from: with two kinds, on 10 November 2026 every instance was
    # taken back within hours for want of capacity, and none could be
    # launched. The allocation strategy picks the cheapest with capacity.
    "cpu": Instances(
        instance_types=[
            "c7i.4xlarge",
            "c7a.4xlarge",
            "c6i.4xlarge",
            "c6a.4xlarge",
            "c5.4xlarge",
            "c5a.4xlarge",
            "m7i.4xlarge",
            "m6i.4xlarge",
            "m6a.4xlarge",
            "m5.4xlarge",
        ],
        image_type=batch.EcsMachineImageType.ECS_AL2023,
        hardware_type=ecs.AmiHardwareType.STANDARD,
        dependency_group="embeddings",
        vcpus=16,
        memory_gib=30,
        gpus=0,
        # Two instances.
        max_vcpus=32,
        workers=4,
        threads=4,
    ),
    # One NVIDIA L4 (g6) or A10G (g5), with 4 or 8 vCPUs. Needs a
    # dependency group with the CUDA build of torch, which pyproject.toml
    # doesn't have yet: the pipeline runs on CPUs.
    "gpu": Instances(
        instance_types=["g6.xlarge", "g6.2xlarge", "g5.xlarge", "g5.2xlarge"],
        image_type=batch.EcsMachineImageType.ECS_AL2023_NVIDIA,
        hardware_type=ecs.AmiHardwareType.GPU,
        dependency_group="embeddings-gpu",
        vcpus=4,
        memory_gib=14,
        gpus=1,
        max_vcpus=16,
        workers=1,
        threads=4,
    ),
}


class DocumentPipeline(Construct):
    """
    The queues, functions, Batch queue and alarms of the document
    pipeline, named `lgsf-<env>-…` like the rest of the stack.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        prefix: str,
        dc_environment: str,
        data_bucket: str,
        data_prefix: str,
        code_exclude: list[str],
        dependencies_layer: aws_lambda.ILayerVersion,
        data_commits_topic: sns.ITopic,
        scraper_notification_topic: sns.ITopic,
        state_machine: sfn.IStateMachine,
    ) -> None:
        super().__init__(scope, construct_id)
        self.prefix = prefix
        self.dc_environment = dc_environment
        self.data_bucket = data_bucket
        self.data_prefix = data_prefix
        self.dependencies_layer = dependencies_layer
        self.scraper_notification_topic = scraper_notification_topic

        kind = self.node.try_get_context("documents-instances") or "cpu"
        assert kind in INSTANCES, (
            f"context `documents-instances` must be one of {list(INSTANCES)}"
        )
        self.instances = INSTANCES[kind]

        # One asset, shared by the three zip functions, like the other zip
        # functions' (the same exclusions, so the same hash).
        self.code = aws_lambda.Code.from_asset(".", exclude=code_exclude)

        self.create_queues()
        self.create_file_ready_topic()
        self.create_batch()
        self.create_functions()
        self.create_triggers(data_commits_topic, state_machine)
        self.create_alarms()

    def name(self, resource_type: str, resource_name: str) -> str:
        return f"{self.prefix}-{resource_type}-{resource_name}"

    # Queues and topic

    def create_queues(self) -> None:
        self.dead_letter_queues: dict[str, sqs.Queue] = {}
        # A Lambda's queue has to hide a message for longer than the
        # function can run, or a slow batch is received twice; six times
        # is AWS's advice, leaving room for the retries the event source
        # makes on a throttled function.
        self.documents_queue = self.create_queue(
            "documents",
            visibility_timeout=cdk.Duration.seconds(DISPATCH_TIMEOUT.to_seconds() * 6),
        )
        self.extract_queue = self.create_queue(
            "extract", visibility_timeout=EXTRACT_VISIBILITY_TIMEOUT
        )
        self.embed_queue = self.create_queue(
            "embed",
            visibility_timeout=cdk.Duration.seconds(EMBED_TIMEOUT.to_seconds() * 6),
        )

    def create_queue(self, name: str, visibility_timeout: cdk.Duration) -> sqs.Queue:
        dead_letter_queue = sqs.Queue(
            self,
            f"{name.title()}DeadLetterQueue",
            queue_name=self.name("queue", f"{name}-dlq"),
            retention_period=QUEUE_RETENTION,
        )
        queue = sqs.Queue(
            self,
            f"{name.title()}Queue",
            queue_name=self.name("queue", name),
            visibility_timeout=visibility_timeout,
            retention_period=QUEUE_RETENTION,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=MAX_RECEIVE_COUNT, queue=dead_letter_queue
            ),
        )
        self.dead_letter_queues[name] = dead_letter_queue
        return queue

    def create_file_ready_topic(self) -> None:
        # One message each time a file's status is written, for whatever
        # reads the results. Like data-commits, a doorbell: the subscriber
        # reads the status file itself. Its ARN goes to SSM for things
        # outside this stack that subscribe, as data-commits' does.
        self.file_ready_topic = sns.Topic(
            self,
            "FileReadyTopic",
            topic_name=self.name("topic", "file-ready"),
            display_name="LGSF document pipeline: file ready",
        )
        ssm.StringParameter(
            self,
            "FileReadyTopicArnParameter",
            parameter_name=f"/lgsf/{self.dc_environment}/sns/file_ready_topic_arn",
            string_value=self.file_ready_topic.topic_arn,
        )

    # Batch, for Docling

    def create_batch(self) -> None:
        instances = self.instances
        stack = cdk.Stack.of(self)

        # The default VPC, named in SSM like the rest of the configuration
        # rather than looked up: a lookup needs AWS credentials when the
        # stack is synthesised, and CI synthesises without any. The
        # parameter types make CloudFormation check the ids exist.
        vpc_id = ssm.StringParameter.value_for_typed_string_parameter_v2(
            self,
            f"/lgsf/{self.dc_environment}/vpc/id",
            ssm.ParameterValueType.AWS_EC2_VPC_ID,
        )
        subnet_ids = ssm.StringListParameter.value_for_typed_list_parameter(
            self,
            f"/lgsf/{self.dc_environment}/vpc/public_subnet_ids",
            ssm.ParameterValueType.AWS_EC2_SUBNET_ID,
        )
        # The subnet ids are a list CloudFormation resolves, so the CDK
        # can't count them: it's told there are three, one for each of
        # eu-west-2's availability zones, as the default VPC has.
        availability_zones = [f"{stack.region}{az}" for az in "abc"]
        vpc = ec2.Vpc.from_vpc_attributes(
            self,
            "Vpc",
            vpc_id=vpc_id,
            availability_zones=availability_zones,
            public_subnet_ids=[
                cdk.Fn.select(i, subnet_ids) for i in range(len(availability_zones))
            ],
        )

        image = ecr_assets.DockerImageAsset(
            self,
            "DocumentsImage",
            directory=".",
            file="Dockerfile.documents",
            platform=ecr_assets.Platform.LINUX_AMD64,
            build_args={"GROUP": instances.dependency_group},
        )

        # Spot: about 40% of the On-Demand price, and a job taken back is
        # retried below, losing only the file it was converting. Nothing
        # runs between jobs.
        compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            "DocumentsComputeEnvironment",
            compute_environment_name=self.name("batch", "documents"),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            instance_types=[
                ec2.InstanceType(name) for name in instances.instance_types
            ],
            use_optimal_instance_classes=False,
            images=[
                batch.EcsMachineImage(
                    image=ecs.EcsOptimizedImage.amazon_linux2023(
                        hardware_type=instances.hardware_type
                    ),
                    image_type=instances.image_type,
                )
            ],
            spot=True,
            allocation_strategy=batch.AllocationStrategy.SPOT_PRICE_CAPACITY_OPTIMIZED,
            minv_cpus=0,
            maxv_cpus=instances.max_vcpus,
            # The image is several GB (torch and Docling's models), more
            # than the AMI's root volume leaves room for.
            launch_template=ec2.LaunchTemplate(
                self,
                "DocumentsLaunchTemplate",
                block_devices=[
                    ec2.BlockDevice(
                        device_name="/dev/xvda",
                        volume=ec2.BlockDeviceVolume.ebs(
                            100, volume_type=ec2.EbsDeviceVolumeType.GP3
                        ),
                    )
                ],
            ),
        )

        # The queue and the job definition share a name, the one the
        # trigger submits to (DOCUMENTS_BATCH_JOB_QUEUE and
        # DOCUMENTS_BATCH_JOB_DEFINITION).
        self.job_queue_name = self.job_definition_name = f"{self.prefix}-documents"
        self.job_queue = batch.JobQueue(
            self,
            "DocumentsJobQueue",
            job_queue_name=self.job_queue_name,
            compute_environments=[
                batch.OrderedComputeEnvironment(
                    compute_environment=compute_environment, order=1
                )
            ],
        )

        self.batch_log_group = aws_logs.LogGroup(
            self,
            "DocumentsBatchLogGroup",
            log_group_name=f"/aws/batch/{self.prefix}-documents",
            retention=aws_logs.RetentionDays.ONE_MONTH,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        self.batch_job_role = self.create_batch_job_role()
        container = batch.EcsEc2ContainerDefinition(
            self,
            "DocumentsContainer",
            image=ecs.ContainerImage.from_docker_image_asset(image),
            cpu=instances.vcpus,
            memory=cdk.Size.gibibytes(instances.memory_gib),
            gpu=instances.gpus or None,
            job_role=self.batch_job_role,
            logging=ecs.LogDrivers.aws_logs(
                stream_prefix="documents", log_group=self.batch_log_group
            ),
            environment={
                **self.pipeline_environment(),
                "AWS_DEFAULT_REGION": stack.region,
                # Without a terminal, Python buffers stdout, so each
                # file's line would reach CloudWatch only when 8KB had
                # built up, or when the job ended.
                "PYTHONUNBUFFERED": "1",
            },
            # Converts the files the extract queue names until it's
            # empty, each worker a process with its own copy of Docling's
            # models. The queue's URL is in the environment too.
            command=[
                "python",
                "manage.py",
                "embeddings",
                "--drain-queue",
                self.extract_queue.queue_url,
                "--source",
                "s3",
                "--workers",
                str(instances.workers),
                "--threads",
                str(instances.threads),
            ],
        )

        self.job_definition = batch.EcsJobDefinition(
            self,
            "DocumentsJobDefinition",
            job_definition_name=self.job_definition_name,
            container=container,
            timeout=cdk.Duration.hours(12),
            # Batch's most: a day of scarce Spot capacity took three hosts
            # from one job.
            retry_attempts=10,
            retry_strategies=[
                # A Spot instance taken back ("Host EC2 (instance ...)
                # terminated"). The next attempt carries on with the
                # queue; the files being converted come back when their
                # visibility timeout ends, each receipt counting one of
                # the five before the dead-letter queue.
                batch.RetryStrategy.of(
                    batch.Action.RETRY, batch.Reason.SPOT_INSTANCE_RECLAIMED
                ),
                # Anything else is a problem to look at, not retry.
                batch.RetryStrategy.of(
                    batch.Action.EXIT, batch.Reason.custom(on_reason="*")
                ),
            ],
        )

    def create_batch_job_role(self) -> aws_iam.Role:
        role = aws_iam.Role(
            self,
            "DocumentsBatchJobRole",
            role_name=self.name("role", "documents-batch"),
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            description="The document pipeline's Batch job: Docling",
        )
        self.grant_documents_read_write(role)
        # Its own queue's messages, and the next stage's queue.
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:ChangeMessageVisibility",
                    "sqs:GetQueueAttributes",
                ],
                resources=[self.extract_queue.queue_arn],
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["sqs:SendMessage"],
                resources=[self.embed_queue.queue_arn],
            )
        )
        self.file_ready_topic.grant_publish(role)
        return role

    # Lambda functions

    def pipeline_environment(self) -> dict[str, str]:
        """What every function and the Batch job are told."""
        return {
            "DC_ENVIRONMENT": self.dc_environment,
            "LGSF_S3_BUCKET": self.data_bucket,
            "LGSF_S3_PREFIX": self.data_prefix,
            "LGSF_DOCUMENT_STORAGE_BACKEND": "s3",
            "DOCUMENTS_QUEUE_URL": self.documents_queue.queue_url,
            "EXTRACT_QUEUE_URL": self.extract_queue.queue_url,
            "EMBED_QUEUE_URL": self.embed_queue.queue_url,
            "FILE_READY_TOPIC_ARN": self.file_ready_topic.topic_arn,
            "DOCUMENTS_BATCH_JOB_QUEUE": self.job_queue_name,
            "DOCUMENTS_BATCH_JOB_DEFINITION": self.job_definition_name,
            "TRIGGER_FUNCTION_NAME": self.trigger_function_name,
            "EMBEDDER": EMBEDDER,
        }

    @property
    def trigger_function_name(self) -> str:
        return self.name("lambda", "documents-trigger")

    def create_functions(self) -> None:
        environment = {
            "PYTHONPATH": "/var/task:/opt/python",
            **self.pipeline_environment(),
        }

        # Finds files to look at and queues them: after a push to
        # LGSF-Data (the doorbell), after every run (the sweep, for
        # anything a doorbell missed), and every hour, when it also
        # submits a Batch job if the extract queue has messages. It
        # invokes itself to carry on past its own timeout.
        since = ssm.StringParameter(
            self,
            "DocumentsSinceParameter",
            parameter_name=f"/lgsf/{self.dc_environment}/documents/since",
            string_value=DOCUMENTS_SINCE,
            description="The document pipeline queues only files stored after this",
        )
        self.trigger_role = self.create_function_role("documents-trigger")
        self.trigger_function = aws_lambda.Function(
            self,
            "DocumentsTriggerFunction",
            function_name=self.trigger_function_name,
            code=self.code,
            handler="lgsf.embeddings.aws.trigger_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            memory_size=512,
            timeout=TRIGGER_TIMEOUT,
            layers=[self.dependencies_layer],
            role=self.trigger_role,
            environment={**environment, "DOCUMENTS_SINCE": since.string_value},
            description="Document pipeline: queue files to dispatch, start Batch",
        )
        stack = cdk.Stack.of(self)
        self.trigger_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                # Its own ARN, from its name, so the policy needn't wait
                # for the function.
                resources=[
                    f"arn:aws:lambda:{stack.region}:{stack.account}:function:"
                    f"{self.trigger_function_name}"
                ],
            )
        )
        self.trigger_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["batch:SubmitJob"],
                resources=[
                    self.job_queue.job_queue_arn,
                    # The job is submitted by name, which IAM evaluates
                    # as the bare job definition ARN; a revision is
                    # allowed too, should one ever be named.
                    f"arn:aws:batch:{stack.region}:{stack.account}:job-definition/"
                    f"{self.job_definition_name}",
                    f"arn:aws:batch:{stack.region}:{stack.account}:job-definition/"
                    f"{self.job_definition_name}:*",
                ],
            )
        )
        # For seeing whether a job is already on the queue before
        # submitting another. ListJobs takes no resource.
        self.trigger_role.add_to_policy(
            aws_iam.PolicyStatement(actions=["batch:ListJobs"], resources=["*"])
        )
        self.documents_queue.grant_send_messages(self.trigger_role)
        self.trigger_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["sqs:GetQueueAttributes"],
                resources=[self.extract_queue.queue_arn],
            )
        )
        # The sweep lists each council's documents and reads their
        # statuses; it writes nothing itself.
        self.grant_documents_read(self.trigger_role)

        # Settles each file without Docling, or sends it on.
        self.dispatch_role = self.create_function_role("documents-dispatch")
        self.dispatch_function = aws_lambda.Function(
            self,
            "DocumentsDispatchFunction",
            function_name=self.name("lambda", "documents-dispatch"),
            code=self.code,
            handler="lgsf.embeddings.aws.dispatch_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            memory_size=1024,
            timeout=DISPATCH_TIMEOUT,
            layers=[self.dependencies_layer],
            role=self.dispatch_role,
            reserved_concurrent_executions=DISPATCH_CONCURRENCY,
            # It downloads whole documents to /tmp to see what they are.
            ephemeral_storage_size=cdk.Size.mebibytes(2048),
            environment=environment,
            description="Document pipeline: settle a file or send it to extract or embed",
        )
        self.dispatch_function.add_event_source(
            event_sources.SqsEventSource(
                self.documents_queue,
                batch_size=10,
                report_batch_item_failures=True,
                max_concurrency=DISPATCH_CONCURRENCY,
            )
        )
        self.grant_documents_read_write(self.dispatch_role)
        self.extract_queue.grant_send_messages(self.dispatch_role)
        self.embed_queue.grant_send_messages(self.dispatch_role)
        self.file_ready_topic.grant_publish(self.dispatch_role)

        # Embeds a file's chunks with Bedrock. Reserved concurrency and
        # the event source's are both EMBED_CONCURRENCY: the event source
        # keeps to it, and the reservation stops anything else (a retry,
        # a call by hand) adding to the load on Titan's quota.
        self.embed_role = self.create_function_role("documents-embed")
        self.embed_function = aws_lambda.Function(
            self,
            "DocumentsEmbedFunction",
            function_name=self.name("lambda", "documents-embed"),
            code=self.code,
            handler="lgsf.embeddings.aws.embed_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            memory_size=1024,
            timeout=EMBED_TIMEOUT,
            layers=[self.dependencies_layer],
            role=self.embed_role,
            reserved_concurrent_executions=EMBED_CONCURRENCY,
            environment=environment,
            description="Document pipeline: embed a file's chunks with Bedrock",
        )
        self.embed_function.add_event_source(
            event_sources.SqsEventSource(
                self.embed_queue,
                batch_size=10,
                report_batch_item_failures=True,
                max_concurrency=EMBED_CONCURRENCY,
            )
        )
        self.embed_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["bedrock:InvokeModel"],
                resources=[TITAN_V2_MODEL_ARN],
            )
        )
        self.grant_documents_read_write(self.embed_role)
        # A file whose chunks have gone goes back to be converted.
        self.extract_queue.grant_send_messages(self.embed_role)
        self.file_ready_topic.grant_publish(self.embed_role)

    def create_function_role(self, name: str) -> aws_iam.Role:
        """A role with only what one function needs, rather than the shared one."""
        return aws_iam.Role(
            self,
            f"{name.title().replace('-', '')}Role",
            role_name=self.name("role", name),
            assumed_by=aws_iam.ServicePrincipal("lambda.amazonaws.com"),
            description=f"Execution role for {self.name('lambda', name)}",
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

    def grant_documents_read(self, role: aws_iam.Role) -> None:
        # Listing a council's documents needs this, and without it a
        # HeadObject of a missing key is a 403 rather than a 404.
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[f"arn:aws:s3:::{self.data_bucket}"],
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"arn:aws:s3:::{self.data_bucket}/{self.data_prefix}/*"],
            )
        )

    def grant_documents_read_write(self, role: aws_iam.Role) -> None:
        # Documents and the files written beside them: read and written,
        # never deleted, as for the scraper worker.
        self.grant_documents_read(role)
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"arn:aws:s3:::{self.data_bucket}/{self.data_prefix}/*"],
            )
        )

    # What starts the trigger

    def create_triggers(
        self, data_commits_topic: sns.ITopic, state_machine: sfn.IStateMachine
    ) -> None:
        # The doorbell: a push to a council's repository, so its new
        # documents are looked at within minutes of being scraped.
        data_commits_topic.add_subscription(
            subscriptions.LambdaSubscription(self.trigger_function)
        )

        # The sweep: once a run has ended, however it ended, so a file a
        # doorbell missed (a lost message, a failed publish) waits a day
        # at most.
        self.sweep_rule = events.Rule(
            self,
            "DocumentsSweepRule",
            rule_name=self.name("rule", "documents-sweep"),
            description="Sweep for documents the pipeline has missed, after each run",
            event_pattern=events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={
                    "status": STATE_MACHINE_END_STATES,
                    "stateMachineArn": [state_machine.state_machine_arn],
                },
            ),
            targets=[targets.LambdaFunction(self.trigger_function)],
        )

        # Every hour: a Batch job for the extract queue when it has
        # messages, and whatever else is due.
        self.hourly_rule = events.Rule(
            self,
            "DocumentsHourlyRule",
            rule_name=self.name("rule", "documents-hourly"),
            description="Start a Batch job when the extract queue has messages",
            schedule=events.Schedule.rate(cdk.Duration.hours(1)),
            targets=[targets.LambdaFunction(self.trigger_function)],
        )

    # Alarms

    def create_alarms(self) -> None:
        # A Batch job that ends in FAILED: a Spot reclaim is retried, so
        # this is a job that ran out of attempts, timed out after 12
        # hours, or stopped on an error. The queue keeps its messages.
        events.Rule(
            self,
            "DocumentsBatchFailedRule",
            rule_name=self.name("rule", "documents-batch-failed"),
            description="Tell someone when a document pipeline Batch job fails",
            event_pattern=events.EventPattern(
                source=["aws.batch"],
                detail_type=["Batch Job State Change"],
                detail={
                    "status": ["FAILED"],
                    "jobQueue": [self.job_queue.job_queue_arn],
                },
            ),
            targets=[
                targets.SnsTopic(
                    self.scraper_notification_topic,
                    message=events.RuleTargetInput.from_text(
                        "Document pipeline Batch job "
                        f"{events.EventField.from_path('$.detail.jobName')} "
                        f"({events.EventField.from_path('$.detail.jobId')}) "
                        f"{events.EventField.from_path('$.detail.status')}: "
                        f"{events.EventField.from_path('$.detail.statusReason')}"
                    ),
                )
            ],
        )

        notify = cloudwatch_actions.SnsAction(self.scraper_notification_topic)
        self.alarms = []
        for name, dead_letter_queue in self.dead_letter_queues.items():
            alarm = cloudwatch.Alarm(
                self,
                f"{name.title()}DeadLetterAlarm",
                alarm_name=self.name("alarm", f"{name}-dlq"),
                alarm_description=(
                    f"The document pipeline's {name} queue has a message it "
                    f"couldn't handle after {MAX_RECEIVE_COUNT} tries"
                ),
                metric=dead_letter_queue.metric_approximate_number_of_messages_visible(
                    period=cdk.Duration.minutes(5), statistic="Maximum"
                ),
                threshold=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                evaluation_periods=1,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            )
            alarm.add_alarm_action(notify)
            self.alarms.append(alarm)

        alarm = cloudwatch.Alarm(
            self,
            "ExtractAgeAlarm",
            alarm_name=self.name("alarm", "extract-age"),
            alarm_description=(
                "A file has waited over six hours to be converted: the "
                "document pipeline's Batch job isn't running or isn't keeping up"
            ),
            metric=self.extract_queue.metric_approximate_age_of_oldest_message(
                period=cdk.Duration.minutes(5), statistic="Maximum"
            ),
            threshold=EXTRACT_AGE_ALARM.to_seconds(),
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )
        alarm.add_alarm_action(notify)
        self.alarms.append(alarm)
