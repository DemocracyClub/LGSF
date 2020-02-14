import os
import re
import pkgutil
from importlib.machinery import SourceFileLoader
from importlib import import_module
import glob
import json

from lgsf.conf import settings


def _abs_path(base_dir, code):
    abs_path = os.path.abspath(base_dir)
    abs_path_root = os.path.join(abs_path, code.upper())
    if os.path.exists(abs_path_root):
        return (abs_path_root, code)
    for file_path in glob.glob("{}/*".format(base_dir)):
        file_name = os.path.split(file_path)[-1]
        if re.match("{}-[a-z\-]+".format(code.upper()), file_name):
            return (file_path, code)
        else:
            parts = file_name.lower().split("-")
            if code.lower() in parts:
                return (file_path, parts[0])
    raise IOError("No scraper file at path")


def scraper_abs_path(code=None):
    return _abs_path(settings.SCRAPER_DIR_NAME, code)[0]


def create_org_package(name):
    path = os.path.join(settings.SCRAPER_DIR_NAME, name)
    os.makedirs(path, exist_ok=True)
    return path


def data_abs_path(code=None, subdir=None):
    try:
        abspath = _abs_path(settings.DATA_DIR_NAME, code)[0]
    except OSError:
        path_args = [x for x in [settings.DATA_DIR_NAME, subdir, code] if x]
        abspath = os.path.abspath(os.path.join(*path_args))
    if subdir:
        abspath = os.path.join(abspath, subdir)
    return abspath


def scraper_path_exists(path):
    return os.path.exists(path)


def load_scraper(code, command):
    from lgsf.scrapers import ScraperBase

    path = os.path.join(scraper_abs_path(code), "{}.py".format(command))
    if not os.path.exists(path):
        return False
    scraper_module = SourceFileLoader("module.name", path).load_module()
    scraper_class = scraper_module.Scraper
    if not issubclass(scraper_class, ScraperBase):
        raise ValueError(
            "Scraper at {} must be a subclass "
            "of lgsf.scrapers.BaseScraper".format(path)
        )
    return scraper_class

def load_council_info(code):
    path = os.path.join(scraper_abs_path(code), "metadata.json")
    if os.path.exists(path):
        return json.loads(open(path).read())

def get_commands():
    command_path = os.path.join(settings.BASE_PATH, "lgsf")
    return [mod.name for mod in pkgutil.iter_modules([command_path])]


def load_command(module_name):
    return import_module(
        "{}.{}".format(module_name, settings.COMMAND_FILE_NAME)
    )
