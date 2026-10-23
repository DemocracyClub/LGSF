"""
Every scraper type's command can start a run on AWS, of that type only.
"""

import io

import pytest

from lgsf.aws_lambda import step_function_invoker
from lgsf.councillors.commands import Command as CouncillorsCommand
from lgsf.decisions.commands import Command as DecisionsCommand
from lgsf.minutes.commands import Command as MinutesCommand
from lgsf.scraper_types import SCRAPER_TYPES


class FakeInvoker:
    calls = []

    def __init__(self, *args, **kwargs):
        self.state_machine_arn = "arn:aws:states:eu-west-2:1:stateMachine:lgsf"

    def invoke(self, **kwargs):
        FakeInvoker.calls.append(kwargs)
        return {
            "execution_arn": "arn:x",
            "start_date": "2026-10-24T06:00:00",
            "execution_name": "n",
            "input": {},
            "state_machine_arn": self.state_machine_arn,
        }


@pytest.fixture(autouse=True)
def fake_invoker(monkeypatch):
    FakeInvoker.calls = []
    monkeypatch.setattr("lgsf.commands.aws_mixin.StepFunctionInvoker", FakeInvoker)
    return FakeInvoker


@pytest.mark.parametrize(
    "command_cls", [CouncillorsCommand, MinutesCommand, DecisionsCommand]
)
def test_aws_runs_only_the_commands_own_type(command_cls):
    argv = ["", "--aws", "--council", "KIR"]
    command = command_cls(argv, io.StringIO())

    command.execute()

    (call,) = FakeInvoker.calls
    assert call["scraper_types"] == [command_cls.command_name]
    assert call["council_ids"] == "KIR"
    assert call["allow_overlap"] is False


def test_every_scraper_command_is_a_registered_type():
    """The type --aws sends must be one the enumerator accepts."""
    for command_cls in (CouncillorsCommand, MinutesCommand, DecisionsCommand):
        assert command_cls.command_name in SCRAPER_TYPES


def test_the_invoker_module_is_untouched_by_the_fake():
    assert step_function_invoker.StepFunctionInvoker is not FakeInvoker
