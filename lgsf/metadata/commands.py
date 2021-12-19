import os
import json

import requests

from lgsf.commands.base import CommandBase
from lgsf.path_utils import scraper_abs_path, create_org_package


class Command(CommandBase):
    def add_arguements(self, parser):
        parser.add_argument(
            "--update",
            action="store",
            help="Update the metadata from Democracy Club",
        )

    def handle(self, options):
        base_url = "https://elections.democracyclub.org.uk/"
        url = "{}api/organisations/".format(base_url)
        while url:
            req = requests.get(url)
            data = req.json()
            url = data.get("next")
            for org in data["results"]:
                if org["organisation_type"] == "local-authority":
                    print(org["official_identifier"])
                    del org["modified"]
                    try:
                        path = scraper_abs_path(org["official_identifier"])
                    except IOError:
                        # This org does't exist yet
                        name = "{}-{}".format(org["official_identifier"], org["slug"])
                        path = create_org_package(name)
                    with open(os.path.join(path, "metadata.json"), "w") as f:
                        f.write(json.dumps(org, indent=4))
                    open(os.path.join(path, "__init__.py"), "a").close()
