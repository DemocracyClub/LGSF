import os
from typing import Type


class BaseSettings(object):
    """
    Django like global settings object.

    Use the class to store global settings for the project.

    Read the settings by calling `from lgsf.conf import settings`.
    """

    def __init__(self):
        "Default settings"

        dir_name = os.path.dirname(__file__)
        self.BASE_PATH = os.path.abspath(os.path.join(dir_name, "..", ".."))
        self.SCRAPER_DIR_NAME = "scrapers"
        self.DATA_DIR_NAME = "data"
        self.COMMAND_FILE_NAME = "commands"

        from lgsf.aws_lambda.run_log import RunLog

        self.RUN_LOGGER: Type[RunLog] = RunLog

        self.APPS = (
            "councillors",
            "templates",
            "metadata",
            # 'parties',
            # "scrapers",
            # 'reconcilers',
        )


settings = BaseSettings()
