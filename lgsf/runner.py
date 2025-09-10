import sys

from lgsf import LOGO
from lgsf.conf import settings
from lgsf.path_utils import load_command


class CommandRunner:
    def __init__(self, argv, stdout=None):
        if not stdout:
            stdout = sys.stdout
        self.stdout = stdout

        try:
            subcommand = argv[1]
            self.get_command_list().index(subcommand)
        except (IndexError, ValueError):
            # Display help if no arguments were given or command not valid
            subcommand = "help"

        if subcommand == "help":
            self.stdout.write(self.format_help())
        else:
            Command = load_command(subcommand)
            Command(argv[1:], self.stdout, pretty=True).execute()

    def format_help(self):
        help_text = [
            LOGO,
            "Local Government Scraper Framework",
            "Usage: manage.py [subcommand]",
            "\nAvailable subcommands:",
        ]
        for command in self.get_command_list():
            help_text.append("\t * {}".format(command))

        return "\n".join(help_text)

    def get_command_list(self):
        """
        Return a list of module names that can be run as a subcommand
        """
        # TODO This should know if an app has valid comand(s)
        return settings.APPS
