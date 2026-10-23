"""
The state machine the CDK renders: one item per council, and within it the
council's scraper types one at a time.

Synthesises the real stack with asset bundling turned off. Needs the cdk
dependency group (`uv sync --group cdk`); skipped without it, as in the
plain test job.
"""

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("aws_cdk")

from aws_cdk import App, Environment  # noqa: E402
from aws_cdk.assertions import Template  # noqa: E402

from lgsf.scraper_types import SCRAPER_TYPES  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "cdk"))
from stacks.lgsf_stack import LgsfStack  # noqa: E402


@pytest.fixture(scope="module")
def template():
    app = App(context={"dc-environment": "development", "aws:cdk:bundling-stacks": []})
    stack = LgsfStack(
        app, "LGSF-Test", env=Environment(account="123456789012", region="eu-west-2")
    )
    return Template.from_stack(stack)


def definition(template):
    """The state machine's ASL, with CloudFormation references replaced by placeholders."""
    (resource,) = template.find_resources("AWS::StepFunctions::StateMachine").values()
    parts = resource["Properties"]["DefinitionString"]["Fn::Join"][1]
    return json.loads("".join(p if isinstance(p, str) else "REF" for p in parts))


def test_each_councils_types_run_one_at_a_time(template):
    states = definition(template)["States"]
    outer = states["ParallelScrapers"]

    assert outer["ItemProcessor"]["ProcessorConfig"]["Mode"] == "DISTRIBUTED"
    assert outer["ItemsPath"] == "$.councils"
    inner = outer["ItemProcessor"]["States"]["RunCouncilScraperTypes"]
    assert inner["Type"] == "Map"
    assert inner["MaxConcurrency"] == 1
    assert inner["ItemsPath"] == "$.scraper_types"
    assert "RunScraper" in inner["ItemProcessor"]["States"]


def test_the_execution_name_is_taken_from_the_parent_execution(template):
    """Inside a Distributed Map item, $$.Execution.Name names the child."""
    states = definition(template)["States"]
    outer = states["ParallelScrapers"]

    assert outer["ItemSelector"]["execution_id.$"] == "$$.Execution.Name"
    inner = outer["ItemProcessor"]["States"]["RunCouncilScraperTypes"]
    assert inner["ItemSelector"] == {
        "council.$": "$.council",
        "scraper_type.$": "$$.Map.Item.Value",
        "execution_id.$": "$.execution_id",
    }
    task = inner["ItemProcessor"]["States"]["RunScraper"]
    assert task["Parameters"]["Payload"]["execution_id.$"] == "$.execution_id"
    assert (
        states["AggregateResults"]["Parameters"]["Payload"]["execution_id.$"]
        == "$$.Execution.Name"
    )


def test_a_failing_type_is_caught_so_the_next_type_runs(template):
    inner = definition(template)["States"]["ParallelScrapers"]["ItemProcessor"][
        "States"
    ]["RunCouncilScraperTypes"]
    task = inner["ItemProcessor"]["States"]["RunScraper"]

    assert task["Catch"][0]["ErrorEquals"] == ["States.ALL"]
    assert task["Catch"][0]["Next"] == "HandleScraperError"


def test_a_log_group_per_registered_type(template):
    names = {
        r["Properties"]["LogGroupName"]
        for r in template.find_resources("AWS::Logs::LogGroup").values()
    }
    for scraper_type in SCRAPER_TYPES:
        assert f"/aws/lambda/lgsf-scrapers/{scraper_type}" in names


def test_the_schedule_starts_every_council_with_the_daily_types(template):
    (rule,) = template.find_resources("AWS::Events::Rule").values()
    (target,) = rule["Properties"]["Targets"]
    assert json.loads(target["Input"]) == {"all_councils": True}
