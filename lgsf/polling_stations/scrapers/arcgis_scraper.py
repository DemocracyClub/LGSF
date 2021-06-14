import json
from arcgis2geojson import arcgis2geojson


from lgsf.polling_stations.scrapers.common import PollingStationScraperBase


class ArcGisScraper(PollingStationScraperBase):
    encoding = "utf-8"
    key = "OBJECTID"

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
