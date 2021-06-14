import abc
import json
from arcgis2geojson import arcgis2geojson
from numbers import Number

from lgsf.polling_stations.scrapers.common import PollingStationScraperBase


class GeoJsonScraper(PollingStationScraperBase):
    encoding = "utf-8"

    def make_geometry(self, feature):
        return json.dumps(arcgis2geojson(feature), sort_keys=True)

    def get_data(self, url):  # pragma: no cover
        response = self.get(url)
        data_str = response.content
        data = json.loads(data_str.decode(self.encoding))
        return data

    def process_feature(self, feature, fields=None):
        # assemble record
        record = {
            "council_id": self.council_id,
            "geometry": self.make_geometry(feature),
        }

        if self.key is None:
            record["pk"] = feature["id"]
        else:
            record["pk"] = feature["properties"][self.key]

        for field in feature["properties"]:
            value = feature["properties"][field]
            if value is None or isinstance(value, Number) or isinstance(value, str):
                if isinstance(value, str):
                    record[field] = value.strip()
                else:
                    record[field] = value

        return record

    def scrape(self, url, type="features"):
        # load json
        data = self.get_data(url)
        print(f"found {len(data['features'])} {type}")

        features = data["features"]

        return self.process_features(features)


class RandomIdGeoJSONScraper(GeoJsonScraper):
    def get_data(self, url):

        """
        Some WFS servers produce output with id fields that seem to
        be randomly generated. Define an id from some other aspect of the feature
        See old wdiv scrapers repo for an alternate approach
        """

        response = self.get(url)
        data_str = response.content
        data = json.loads(data_str.decode(self.encoding))

        for i in range(0, len(data["features"])):
            data["features"][i]["id"] = self.make_pk(data["features"][i])

        return data

    def make_pk(self, feature):
        raise NotImplementedError
