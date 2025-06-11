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
            "\n" "Available subcommands:",
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

    # parser = argparse.ArgumentParser(argv)
    #
    # parser.add_argument('--run', nargs='+')
    # parser.add_argument('--check', nargs='+')
    # parser.add_argument('--offline', default=False, action='store_true')
    # options = parser.parse_args()
    #
    # if options.run:
    #     self.do_run(options, 'run')
    #
    # if options.check:
    #     self.do_run(options, 'check')

    # def do_run(self, options, op):
    #
    #     for code in getattr(options, op):
    #         code = code.upper()
    #         scraper_path = scraper_abs_path(code)
    #         if scraper_path_exists(scraper_path):
    #             # Run a scraper somehow!
    #             scraper_class = load_scraper(code)
    #             scraper_class
    #             scraper_instance = scraper_class(options)
    #             getattr(scraper_instance, op)()
    #         else:
    #             raise ValueError("Path {} does not exist".format(
    #                 scraper_path
    #             ))
