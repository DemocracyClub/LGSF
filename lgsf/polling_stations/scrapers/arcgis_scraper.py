import json
from arcgis2geojson import arcgis2geojson

from lgsf.polling_stations.models import PollingStationsList, PollingDistrictsList
from lgsf.scrapers.base import ScraperBase
from lgsf.polling_stations.scrapers.common import (
    # BaseScraper,
    # get_data_from_url,
    # save,
    summarise,
    sync_db_to_github,
    truncate,
    PollingStationScraperBase,
)


class ArcGisScraper(PollingStationScraperBase):
    def make_geometry(self, feature):
        return json.dumps(arcgis2geojson(feature), sort_keys=True)

    def get_data(self, url):  # pragma: no cover
        response = self.get(url)
        data_str = response.content
        data = json.loads(data_str.decode(self.encoding))
        return (data_str, data)

    def process_feature(self, feature, fields=None):
        # assemble record
        record = {
            "council_id": self.council_id,
            "geometry": self.make_geometry(feature),
        }
        for field in fields:
            value = feature["attributes"][field["name"]]
            if isinstance(value, str):
                record[field["name"]] = value.strip()
            else:
                record[field["name"]] = value
        return record

    def scrape(self, url, type="features"):
        # load json
        data_str, data = self.get_data(url)
        print(f"found {len(data['features'])} {type}")

        # grab field names
        fields = data["fields"]
        features = data["features"]

        return self.process_features(features, fields)

        # print summary
        # summarise(self.table)

        # self.store_history(data_str, self.council_id)
