import os

from lgsf.commands.base import CommandBase
from lgsf.path_utils import _abs_path
from lgsf.conf import settings

from .helpers import TEMPLATES


class Command(CommandBase):
    command_name = "templates"

    def add_arguments(self, parser):
        parser.add_argument(
            "--council",
            action="store",
            required=True,
            help="The council ID to save the template in",
        )
        parser.add_argument(
            "--template", action="store", help="The name of the template"
        )
        parser.add_argument(
            "--context",
            nargs=2,
            action="append",
            help="The context to pass to the template",
        )

    def handle(self, options):
        template_name = options["template"]
        if template_name not in TEMPLATES.keys():
            template_options = "\n\t * ".join(TEMPLATES.keys())
            raise ValueError(
                "{} is not a valid template name, options are:\n\t* {}".format(
                    template_name, template_options
                )
            )
        context = dict(options["context"])
        template = TEMPLATES[template_name](context)
        scraper_text = template.format_template()

        path, code = _abs_path(settings.SCRAPER_DIR_NAME, options["council"])
        scraper_path = os.path.join(path, template.file_name)
        if os.path.exists(scraper_path):
            raise ValueError("Scraper already exists, not overwriting")
        with open(scraper_path, "w") as f:
            f.write(scraper_text)
