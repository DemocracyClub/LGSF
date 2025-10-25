from lgsf.commands.aws_mixin import AWSInvokableMixin
from lgsf.commands.base import PerCouncilCommandBase


class Command(AWSInvokableMixin, PerCouncilCommandBase):
    command_name = "councillors"
