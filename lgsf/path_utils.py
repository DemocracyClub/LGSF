import os
import pkgutil
from importlib.machinery import SourceFileLoader
from importlib import import_module
import glob

from lgsf.conf import settings


def _abs_path(base_dir, code):
    abs_path = os.path.abspath(base_dir)
    if code:
        abs_path_root = os.path.join(abs_path, code.upper())
        try:
            abs_path = glob.glob("{}*".format(abs_path_root))[0]
        except IndexError:
            # This might be a slug rather than a code
            abs_path = glob.glob("{}/*{}*".format(abs_path, code.lower()))[0]

    code = os.path.split(abs_path)[-1].split("-")[0]
    return (abs_path, code)


def scraper_abs_path(code=None):
    return _abs_path(settings.SCRAPER_DIR_NAME, code)[0]


def data_abs_path(code=None, subdir=None):
    abspath = _abs_path(settings.DATA_DIR_NAME, code)[0]
    if subdir:
        abspath = os.path.join(abspath, subdir)
    return abspath


def scraper_path_exists(path):
    return os.path.exists(path)


def load_scraper(code, command):
    from lgsf.scrapers import ScraperBase

    path = os.path.join(scraper_abs_path(code), "{}.py".format(command))
    scraper_module = SourceFileLoader("module.name", path).load_module()
    scraper_class = scraper_module.Scraper
    if not issubclass(scraper_class, ScraperBase):
        raise ValueError(
            "Scraper at {} must be a subclass "
            "of lgsf.scrapers.BaseScraper".format(path)
        )
    return scraper_class


def get_commands():
    command_path = os.path.join(settings.BASE_PATH, "lgsf")
    return [mod.name for mod in pkgutil.iter_modules([command_path])]


def load_command(module_name):
    return import_module(
        "{}.{}".format(module_name, settings.COMMAND_FILE_NAME)
    )
