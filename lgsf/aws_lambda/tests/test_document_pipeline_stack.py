"""
The document pipeline's part of the stack (cdk/stacks/documents.py): the
queues, the three functions and what starts them, the Batch job that
runs Docling, the alarms, and the permissions each piece has.

Synthesises the real stack with asset bundling turned off, as
test_state_machine_definition does. Needs the cdk dependency group
(`uv sync --group cdk`); skipped without it.
"""

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("aws_cdk")

from aws_cdk import App, Environment  # noqa: E402
from aws_cdk.assertions import Template  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "cdk"))
from stacks.lgsf_stack import LgsfStack  # noqa: E402

PREFIX = "lgsf-development"
PIPELINE_VARIABLES = {
    "LGSF_S3_BUCKET",
    "LGSF_S3_PREFIX",
    "LGSF_DOCUMENT_STORAGE_BACKEND",
    "DOCUMENTS_QUEUE_URL",
    "EXTRACT_QUEUE_URL",
    "EMBED_QUEUE_URL",
    "FILE_READY_TOPIC_ARN",
    "DOCUMENTS_BATCH_JOB_QUEUE",
    "DOCUMENTS_BATCH_JOB_DEFINITION",
    "TRIGGER_FUNCTION_NAME",
    "EMBEDDER",
}


@pytest.fixture(scope="module")
def template():
    app = App(context={"dc-environment": "development", "aws:cdk:bundling-stacks": []})
    stack = LgsfStack(
        app, "LGSF-Test", env=Environment(account="123456789012", region="eu-west-2")
    )
    return Template.from_stack(stack)


def resource(template, resource_type, **properties):
    """The one resource of the type whose properties include these."""
    ((logical_id, found),) = template.find_resources(
        resource_type, {"Properties": properties}
    ).items()
    return logical_id, found["Properties"]


def queue(template, name):
    return resource(template, "AWS::SQS::Queue", QueueName=f"{PREFIX}-queue-{name}")


def function(template, name):
    return resource(
        template, "AWS::Lambda::Function", FunctionName=f"{PREFIX}-lambda-{name}"
    )


def actions_of(template, function_properties):
    """Every (action, resource) the function's role allows, from its policies."""
    (role_id,) = function_properties["Role"]["Fn::GetAtt"][:1]
    allowed = []
    for policy in template.find_resources("AWS::IAM::Policy").values():
        if {"Ref": role_id} not in policy["Properties"]["Roles"]:
            continue
        for statement in policy["Properties"]["PolicyDocument"]["Statement"]:
            actions = statement["Action"]
            for action in actions if isinstance(actions, list) else [actions]:
                allowed.append((action, json.dumps(statement["Resource"])))
    return allowed


# Queues and topic


@pytest.mark.parametrize(
    "name, visibility_timeout",
    [
        ("documents", 6 * 5 * 60),
        ("extract", 2 * 60 * 60),
        ("embed", 6 * 15 * 60),
    ],
)
def test_each_queue_has_a_dead_letter_queue(template, name, visibility_timeout):
    _, properties = queue(template, name)
    dlq_id, dlq = queue(template, f"{name}-dlq")

    assert properties["VisibilityTimeout"] == visibility_timeout
    assert properties["RedrivePolicy"] == {
        "deadLetterTargetArn": {"Fn::GetAtt": [dlq_id, "Arn"]},
        "maxReceiveCount": 5,
    }
    # Both keep a message for SQS's longest: a file waits, never expires
    assert properties["MessageRetentionPeriod"] == 14 * 24 * 60 * 60
    assert dlq["MessageRetentionPeriod"] == 14 * 24 * 60 * 60


def test_the_file_ready_topic_is_published_in_ssm_and_the_outputs(template):
    topic_id, _ = resource(
        template, "AWS::SNS::Topic", TopicName=f"{PREFIX}-topic-file-ready"
    )
    _, parameter = resource(
        template,
        "AWS::SSM::Parameter",
        Name="/lgsf/development/sns/file_ready_topic_arn",
    )
    assert parameter["Value"] == {"Ref": topic_id}
    outputs = template.to_json()["Outputs"]
    assert outputs["FileReadyTopicArn"]["Value"] == {"Ref": topic_id}


# The functions


@pytest.mark.parametrize(
    "name, handler, memory, timeout",
    [
        ("documents-trigger", "lgsf.embeddings.aws.trigger_handler", 512, 900),
        ("documents-dispatch", "lgsf.embeddings.aws.dispatch_handler", 1024, 300),
        ("documents-embed", "lgsf.embeddings.aws.embed_handler", 1024, 900),
    ],
)
def test_each_function_runs_its_handler_with_the_pipelines_environment(
    template, name, handler, memory, timeout
):
    _, properties = function(template, name)
    env = properties["Environment"]["Variables"]

    assert properties["Handler"] == handler
    assert properties["Runtime"] == "python3.12"
    assert properties["MemorySize"] == memory
    assert properties["Timeout"] == timeout
    assert properties["Layers"]
    assert PIPELINE_VARIABLES <= set(env)
    assert env["LGSF_S3_PREFIX"] == "merged-data"
    assert env["LGSF_DOCUMENT_STORAGE_BACKEND"] == "s3"
    assert env["EMBEDDER"] == "titan-v2"
    assert env["TRIGGER_FUNCTION_NAME"] == f"{PREFIX}-lambda-documents-trigger"
    assert env["DOCUMENTS_BATCH_JOB_QUEUE"] == f"{PREFIX}-documents"
    assert env["DOCUMENTS_BATCH_JOB_DEFINITION"] == f"{PREFIX}-documents"
    assert env["DOCUMENTS_QUEUE_URL"] == {"Ref": queue(template, "documents")[0]}
    assert env["EXTRACT_QUEUE_URL"] == {"Ref": queue(template, "extract")[0]}
    assert env["EMBED_QUEUE_URL"] == {"Ref": queue(template, "embed")[0]}


def test_the_functions_have_their_own_roles(template):
    roles = {
        function(template, name)[1]["Role"]["Fn::GetAtt"][0]
        for name in ("documents-trigger", "documents-dispatch", "documents-embed")
    }
    assert len(roles) == 3
    assert "LGSFLambdaExecutionRole" not in roles


def test_the_trigger_is_rung_by_data_commits(template):
    topic_id, _ = resource(
        template, "AWS::SNS::Topic", TopicName=f"{PREFIX}-topic-data-commits"
    )
    trigger_id, _ = function(template, "documents-trigger")
    _, subscription = resource(
        template, "AWS::SNS::Subscription", TopicArn={"Ref": topic_id}
    )
    assert subscription["Protocol"] == "lambda"
    assert subscription["Endpoint"] == {"Fn::GetAtt": [trigger_id, "Arn"]}


def test_the_trigger_sweeps_after_every_run_however_it_ended(template):
    trigger_id, _ = function(template, "documents-trigger")
    _, rule = resource(
        template, "AWS::Events::Rule", Name=f"{PREFIX}-rule-documents-sweep"
    )
    (state_machine_id,) = template.find_resources("AWS::StepFunctions::StateMachine")

    assert rule["EventPattern"] == {
        "source": ["aws.states"],
        "detail-type": ["Step Functions Execution Status Change"],
        "detail": {
            "status": ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"],
            "stateMachineArn": [{"Ref": state_machine_id}],
        },
    }
    assert rule["Targets"][0]["Arn"] == {"Fn::GetAtt": [trigger_id, "Arn"]}


def test_the_trigger_runs_every_hour(template):
    trigger_id, _ = function(template, "documents-trigger")
    _, rule = resource(
        template, "AWS::Events::Rule", Name=f"{PREFIX}-rule-documents-hourly"
    )
    assert rule["ScheduleExpression"] == "rate(1 hour)"
    assert rule["Targets"][0]["Arn"] == {"Fn::GetAtt": [trigger_id, "Arn"]}


def test_the_trigger_may_invoke_itself_and_submit_batch_jobs(template):
    _, properties = function(template, "documents-trigger")
    allowed = actions_of(template, properties)

    (invoke,) = [r for a, r in allowed if a == "lambda:InvokeFunction"]
    assert f"function:{PREFIX}-lambda-documents-trigger" in invoke
    (submit,) = [r for a, r in allowed if a == "batch:SubmitJob"]
    assert f"job-definition/{PREFIX}-documents" in submit
    assert f"job-definition/{PREFIX}-documents:*" in submit
    assert "JobQueueArn" in submit
    assert "batch:ListJobs" in {a for a, _ in allowed}
    assert "bedrock:InvokeModel" not in {a for a, _ in allowed}


def test_the_trigger_is_told_where_the_backfill_ends(template):
    """Files stored before DOCUMENTS_SINCE are the backfill command's."""
    _, properties = function(template, "documents-trigger")
    parameter_id, parameter = resource(
        template, "AWS::SSM::Parameter", Name="/lgsf/development/documents/since"
    )
    assert parameter["Value"] == "2026-11-09T00:00:00Z"
    assert properties["Environment"]["Variables"]["DOCUMENTS_SINCE"] == {
        "Fn::GetAtt": [parameter_id, "Value"]
    }
    for name in ("documents-dispatch", "documents-embed"):
        _, other = function(template, name)
        assert "DOCUMENTS_SINCE" not in other["Environment"]["Variables"]


def test_the_trigger_only_reads_documents(template):
    _, properties = function(template, "documents-trigger")
    s3_actions = {a for a, _ in actions_of(template, properties) if a.startswith("s3:")}
    assert s3_actions == {"s3:ListBucket", "s3:GetObject"}


@pytest.mark.parametrize(
    "name, queue_name, concurrency",
    [
        ("documents-dispatch", "documents", 10),
        ("documents-embed", "embed", 4),
    ],
)
def test_the_queue_functions_report_failures_by_item_and_keep_to_a_ceiling(
    template, name, queue_name, concurrency
):
    """The event source keeps to the ceiling; the reservation makes it one."""
    function_id, properties = function(template, name)
    queue_id, _ = queue(template, queue_name)
    _, mapping = resource(
        template,
        "AWS::Lambda::EventSourceMapping",
        FunctionName={"Ref": function_id},
    )

    assert mapping["EventSourceArn"] == {"Fn::GetAtt": [queue_id, "Arn"]}
    assert mapping["BatchSize"] == 10
    assert mapping["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert mapping["ScalingConfig"] == {"MaximumConcurrency": concurrency}
    assert properties["ReservedConcurrentExecutions"] == concurrency


def test_the_dispatch_function_sends_to_both_queues_and_rings_file_ready(template):
    _, properties = function(template, "documents-dispatch")
    allowed = actions_of(template, properties)
    extract_id, _ = queue(template, "extract")
    embed_id, _ = queue(template, "embed")
    topic_id, _ = resource(
        template, "AWS::SNS::Topic", TopicName=f"{PREFIX}-topic-file-ready"
    )

    # Whole documents are downloaded to /tmp to see what they are
    assert properties["EphemeralStorage"] == {"Size": 2048}
    sends = " ".join(r for a, r in allowed if a == "sqs:SendMessage")
    assert extract_id in sends and embed_id in sends
    assert any(a == "sns:Publish" and topic_id in r for a, r in allowed)
    s3_actions = {a for a, _ in allowed if a.startswith("s3:")}
    assert s3_actions == {"s3:ListBucket", "s3:GetObject", "s3:PutObject"}


def test_the_embed_function_is_capped_for_titans_quota(template):
    """Reserved concurrency and the event source agree, so nothing else adds load."""
    _, properties = function(template, "documents-embed")
    assert properties["ReservedConcurrentExecutions"] == 4


def test_the_embed_function_has_only_what_embedding_needs(template):
    """Titan on Bedrock, the documents, file-ready, its queue, and extract
    for a file whose chunks have gone; nothing else."""
    _, properties = function(template, "documents-embed")
    allowed = actions_of(template, properties)
    extract_id, _ = queue(template, "extract")
    embed_id, _ = queue(template, "embed")
    documents_id, _ = queue(template, "documents")
    topic_id, _ = resource(
        template, "AWS::SNS::Topic", TopicName=f"{PREFIX}-topic-file-ready"
    )

    assert {a for a, _ in allowed} == {
        "bedrock:InvokeModel",
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "sns:Publish",
        # Its own queue, from the event source's grant
        "sqs:ReceiveMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueUrl",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes",
        "sqs:SendMessage",
    }
    (bedrock,) = [r for a, r in allowed if a == "bedrock:InvokeModel"]
    assert bedrock == json.dumps(
        "arn:aws:bedrock:eu-west-2::foundation-model/amazon.titan-embed-text-v2:0"
    )
    sends = " ".join(r for a, r in allowed if a == "sqs:SendMessage")
    assert extract_id in sends
    assert embed_id not in sends and documents_id not in sends
    receives = " ".join(r for a, r in allowed if a == "sqs:ReceiveMessage")
    assert embed_id in receives and extract_id not in receives
    assert all(topic_id in r for a, r in allowed if a == "sns:Publish")


# Batch


def test_the_batch_job_drains_the_extract_queue_on_sixteen_cpus(template):
    _, definition = resource(
        template, "AWS::Batch::JobDefinition", JobDefinitionName=f"{PREFIX}-documents"
    )
    extract_id, _ = queue(template, "extract")
    container = definition["ContainerProperties"]

    assert container["Command"] == [
        "python",
        "manage.py",
        "embeddings",
        "--drain-queue",
        {"Ref": extract_id},
        "--source",
        "s3",
        "--workers",
        "4",
        "--threads",
        "4",
    ]
    requirements = {r["Type"]: r["Value"] for r in container["ResourceRequirements"]}
    assert requirements == {"VCPU": "16", "MEMORY": str(30 * 1024)}
    env = {e["Name"]: e["Value"] for e in container["Environment"]}
    assert PIPELINE_VARIABLES <= set(env)
    assert env["EXTRACT_QUEUE_URL"] == {"Ref": extract_id}
    assert definition["Timeout"] == {"AttemptDurationSeconds": 12 * 60 * 60}
    assert definition["RetryStrategy"]["Attempts"] == 3
    assert definition["RetryStrategy"]["EvaluateOnExit"][0] == {
        "Action": "RETRY",
        "OnStatusReason": "Host EC2*",
    }


def test_the_batch_compute_environment_is_spot_cpu_instances(template):
    (environment,) = template.find_resources("AWS::Batch::ComputeEnvironment").values()
    resources = environment["Properties"]["ComputeResources"]

    assert resources["Type"] == "SPOT"
    assert resources["AllocationStrategy"] == "SPOT_PRICE_CAPACITY_OPTIMIZED"
    assert resources["InstanceTypes"] == ["c7i.4xlarge", "c7a.4xlarge"]
    assert resources["MinvCpus"] == 0
    assert resources["MaxvCpus"] == 32
    _, queue_ = resource(
        template, "AWS::Batch::JobQueue", JobQueueName=f"{PREFIX}-documents"
    )
    assert queue_["ComputeEnvironmentOrder"][0]["Order"] == 1


def test_the_batch_job_reads_extract_and_writes_embed(template):
    _, definition = resource(
        template, "AWS::Batch::JobDefinition", JobDefinitionName=f"{PREFIX}-documents"
    )
    allowed = actions_of(
        template, {"Role": definition["ContainerProperties"]["JobRoleArn"]}
    )
    extract_id, _ = queue(template, "extract")
    embed_id, _ = queue(template, "embed")

    receives = " ".join(r for a, r in allowed if a == "sqs:ReceiveMessage")
    assert extract_id in receives and embed_id not in receives
    sends = " ".join(r for a, r in allowed if a == "sqs:SendMessage")
    assert embed_id in sends and extract_id not in sends
    assert "sns:Publish" in {a for a, _ in allowed}
    s3_actions = {a for a, _ in allowed if a.startswith("s3:")}
    assert s3_actions == {"s3:ListBucket", "s3:GetObject", "s3:PutObject"}


# Alarms


def test_a_failed_batch_job_is_reported(template):
    topic_id, _ = resource(
        template,
        "AWS::SNS::Topic",
        TopicName=f"{PREFIX}-topic-scraper-notifications",
    )
    (job_queue_id,) = template.find_resources("AWS::Batch::JobQueue")
    _, rule = resource(
        template, "AWS::Events::Rule", Name=f"{PREFIX}-rule-documents-batch-failed"
    )

    assert rule["EventPattern"] == {
        "source": ["aws.batch"],
        "detail-type": ["Batch Job State Change"],
        "detail": {
            "status": ["FAILED"],
            "jobQueue": [{"Fn::GetAtt": [job_queue_id, "JobQueueArn"]}],
        },
    }
    (target,) = rule["Targets"]
    assert target["Arn"] == {"Ref": topic_id}
    transformer = target["InputTransformer"]
    assert set(transformer["InputPathsMap"].values()) >= {
        "$.detail.jobName",
        "$.detail.status",
        "$.detail.statusReason",
    }
    assert "Batch job" in transformer["InputTemplate"]
    # EventBridge may publish to the topic
    (policy,) = template.find_resources(
        "AWS::SNS::TopicPolicy", {"Properties": {"Topics": [{"Ref": topic_id}]}}
    ).values()
    assert policy["Properties"]["PolicyDocument"]["Statement"][0]["Principal"] == {
        "Service": "events.amazonaws.com"
    }


def test_each_dead_letter_queue_and_the_extract_queues_age_alarm_someone(template):
    topic_id, _ = resource(
        template,
        "AWS::SNS::Topic",
        TopicName=f"{PREFIX}-topic-scraper-notifications",
    )
    alarms = {
        a["Properties"]["AlarmName"]: a["Properties"]
        for a in template.find_resources("AWS::CloudWatch::Alarm").values()
    }

    for name in ("documents", "extract", "embed"):
        alarm = alarms[f"{PREFIX}-alarm-{name}-dlq"]
        dlq_id, _ = queue(template, f"{name}-dlq")
        assert alarm["MetricName"] == "ApproximateNumberOfMessagesVisible"
        assert alarm["Threshold"] == 1
        assert alarm["ComparisonOperator"] == "GreaterThanOrEqualToThreshold"
        assert alarm["Dimensions"] == [
            {"Name": "QueueName", "Value": {"Fn::GetAtt": [dlq_id, "QueueName"]}}
        ]
        assert alarm["AlarmActions"] == [{"Ref": topic_id}]

    age = alarms[f"{PREFIX}-alarm-extract-age"]
    assert age["MetricName"] == "ApproximateAgeOfOldestMessage"
    assert age["Threshold"] == 6 * 60 * 60
    assert age["ComparisonOperator"] == "GreaterThanThreshold"
    assert age["AlarmActions"] == [{"Ref": topic_id}]
    assert len(alarms) == 4
