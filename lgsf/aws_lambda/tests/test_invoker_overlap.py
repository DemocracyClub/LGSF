"""
The CLI's --aws run refuses to overlap a running execution.

Two runs scraping the same council at once end with one push rejected, so
the invoker checks what is running first. The Step Functions client is a
fake answering list_executions and describe_execution.
"""

import json

import pytest

from lgsf.aws_lambda.step_function_invoker import (
    ExecutionOverlapError,
    StepFunctionInvoker,
)

ARN = "arn:aws:states:eu-west-2:123456789012:stateMachine:lgsf"


class FakeStepFunctions:
    def __init__(self, running):
        """running: {name: input dict}"""
        self.running = running
        self.started = []

    class _Paginator:
        def __init__(self, executions):
            self.executions = executions

        def paginate(self, **kwargs):
            assert kwargs["statusFilter"] == "RUNNING"
            yield {"executions": self.executions}

    def get_paginator(self, name):
        assert name == "list_executions"
        return self._Paginator(
            [{"name": n, "executionArn": f"{ARN}:{n}"} for n in self.running]
        )

    def describe_execution(self, executionArn):
        name = executionArn.rsplit(":", 1)[1]
        return {"input": json.dumps(self.running[name])}

    def start_execution(self, **kwargs):
        self.started.append(kwargs)
        return {"executionArn": f"{ARN}:{kwargs['name']}", "startDate": FakeDate()}


class FakeDate:
    def isoformat(self):
        return "2026-10-23T06:00:00"


def invoker(running):
    inv = StepFunctionInvoker(state_machine_arn=ARN)
    inv._client = FakeStepFunctions(running)
    return inv


def test_nothing_running_means_the_execution_starts():
    inv = invoker({})
    result = inv.invoke(council_ids="ABC")
    assert result["input"] == {"council_ids": "ABC"}
    assert len(inv._client.started) == 1


def test_a_running_daily_run_blocks_any_council():
    inv = invoker({"scraper-all-20261023-060000": {"all_councils": True}})

    with pytest.raises(
        ExecutionOverlapError, match="scraper-all-20261023-060000 \\(all councils\\)"
    ):
        inv.invoke(council_ids="ABC")
    assert inv._client.started == []


def test_a_running_council_blocks_only_that_council():
    inv = invoker({"scraper-ABC-20261023-090000": {"council_ids": "ABC,DEF"}})

    with pytest.raises(ExecutionOverlapError, match="\\(ABC, DEF\\)"):
        inv.invoke(council_ids="def")  # case doesn't matter
    inv.invoke(council_ids="XYZ")
    assert len(inv._client.started) == 1


def test_an_all_councils_run_is_blocked_by_anything_running():
    inv = invoker({"scraper-XYZ-20261023-090000": {"council_ids": "XYZ"}})

    with pytest.raises(ExecutionOverlapError):
        inv.invoke(all_councils=True)


def test_allow_overlap_starts_anyway():
    inv = invoker({"scraper-all-20261023-060000": {"all_councils": True}})

    inv.invoke(council_ids="ABC", allow_overlap=True)

    assert len(inv._client.started) == 1


def test_the_message_says_how_to_proceed():
    inv = invoker({"scraper-all-20261023-060000": {"all_councils": True}})
    with pytest.raises(ExecutionOverlapError) as raised:
        inv.invoke(council_ids="ABC")
    assert "--allow-overlap" in str(raised.value)
    assert isinstance(
        raised.value, ValueError
    )  # the CLI already prints ValueErrors plainly


def test_the_scheduled_run_has_no_councils_in_its_input_and_still_blocks():
    """EventBridge starts the daily run with no council list: that's every council."""
    inv = invoker({"daily-6f2a": {}})

    with pytest.raises(ExecutionOverlapError, match="daily-6f2a \\(all councils\\)"):
        inv.invoke(council_ids="ABC")


def test_another_type_running_on_the_council_still_counts_as_an_overlap():
    """A council's types are only kept apart within one execution."""
    inv = invoker(
        {"councillors-KIR-1": {"council_ids": "KIR", "scraper_types": ["councillors"]}}
    )

    with pytest.raises(ExecutionOverlapError):
        inv.invoke(council_ids="KIR", scraper_types=["minutes"])


def test_the_scraper_types_go_in_the_input_and_the_name():
    inv = invoker({})

    result = inv.invoke(council_ids="KIR", scraper_types=["minutes"])

    assert result["input"] == {"council_ids": "KIR", "scraper_types": ["minutes"]}
    assert result["execution_name"].startswith("minutes-KIR-")
