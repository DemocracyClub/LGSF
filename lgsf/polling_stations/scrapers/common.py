import abc
import datetime
import json
import os

# import scraperwiki
import urllib.request
from collections import OrderedDict

# from commitment import GitHubCredentials, GitHubClient
from hashlib import sha1
from retry import retry
from urllib.error import HTTPError

from lgsf.polling_stations.models import PollingStationsList, PollingDistrictsList
from lgsf.scrapers.base import ScraperBase


def truncate(table):
    pass


def summarise(table):
    pass


def get_github_credentials():
    return GitHubCredentials(
        repo=os.environ["MORPH_GITHUB_POLLING_REPO"],
        name=os.environ["MORPH_GITHUB_USERNAME"],
        email=os.environ["MORPH_GITHUB_EMAIL"],
        api_key=os.environ["MORPH_GITHUB_API_KEY"],
    )


def format_json(json_str, exclude_keys=None):
    data = json.loads(json_str, object_pairs_hook=OrderedDict)
    if isinstance(data, dict) and exclude_keys:
        for key in exclude_keys:
            data.pop(key, None)
    return json.dumps(data, sort_keys=True, indent=4)


def sync_file_to_github(council_id, file_name, content):
    try:
        creds = get_github_credentials()
        g = GitHubClient(creds)
        path = "%s/%s" % (council_id, file_name)

        # if we haven't specified an extension, assume .json
        if "." not in path:
            path = "%s.json" % (path)

        g.push_file(
            content, path, "Update %s at %s" % (path, str(datetime.datetime.now()))
        )
    except KeyError:
        # if no credentials are defined in env vars
        # just ignore this step
        pass


def sync_db_to_github(council_id, table_name, key):
    # content = dump_table_to_json(table_name, key)
    # sync_file_to_github(council_id, table_name, content)
    pass


class PollingStationScraperBase(ScraperBase, metaclass=abc.ABCMeta):

    store_raw_data = False

    def __init__(self, options, console):
        super().__init__(options, console)
        self.council_id = self.options["council"]
        self.stations = []
        self.districts = []
        import ipdb

        ipdb.set_trace()

    def save_stations(self):
        station_list = PollingStationsList(self.stations)
        # sync_db_to_github(self.council_id, self.table, self.key)
        self.save_json(station_list)

    def save_districts(self):
        districts_list = PollingDistrictsList(self.districts)
        # sync_db_to_github(self.council_id, self.table, self.key)
        self.save_json(districts_list)

    def run(self):
        if self.stations_url:
            self.stations = self.scrape(self.stations_url, "stations")
            self.save_stations()
        if self.districts_url:
            self.districts = self.scrape(self.districts_url, "districts")
            self.save_districts()

    def process_features(self, features, fields=None):
        feature_list = []
        for feature in features:
            record = self.process_feature(feature, fields)
            feature_list.append(record)
        return feature_list

    @abc.abstractmethod
    def scrape(self, url, type="features"):
        pass

    @abc.abstractmethod
    def get_data(self, url):
        pass

    @abc.abstractmethod
    def process_feature(self, feature, fields=None):
        pass
