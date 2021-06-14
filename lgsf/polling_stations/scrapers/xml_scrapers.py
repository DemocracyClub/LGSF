import abc
import copy

from lxml import etree

from lgsf.polling_stations.scrapers.common import PollingStationScraperBase


class XmlScraper(PollingStationScraperBase, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def make_geometry(self, xmltree, element):
        pass

    @property
    @abc.abstractmethod
    def feature_tag(self):
        pass

    def get_data(self, url):
        response = self.get(url)
        return response.content

    def process_features(self, features, tree, fields):
        feature_list = []
        for feature in features:
            record = self.process_feature(feature, tree, fields)
            feature_list.append(record)
        return feature_list

    def process_feature(self, feature, tree, fields):
        record = {
            "council_id": self.council_id,
            "geometry": self.make_geometry(tree, feature),
        }

        # extract attributes and assemble record
        for attribute in feature[0]:
            if attribute.tag in fields:
                if isinstance(attribute.text, str):
                    record[fields[attribute.tag]] = attribute.text.strip()
                else:
                    record[fields[attribute.tag]] = attribute.text
        return record

    def scrape(self, url, type="features", fields=None):
        # load xml
        data_str = self.get_data(url)
        tree = etree.fromstring(data_str)
        features = tree.findall(self.feature_tag)
        print(f"found {len(features)} {type}")

        return self.process_features(features, tree, fields)

    def run(self):
        if self.stations_url:
            self.scrape(self.stations_url, type="stations", fields=self.stations_fields)
        if self.districts_url:
            self.scrape(
                self.districts_url, type="districts", fields=self.districts_fields
            )

    def dump_fields(self):  # pragma: no cover
        data_str = self.get_data()
        tree = etree.fromstring(data_str)
        features = tree.findall(self.feature_tag)
        for attribute in features[0][0]:
            print(attribute.tag)


class GmlScraper(XmlScraper):

    feature_tag = "{http://www.opengis.net/gml}featureMember"

    def make_geometry(self, xmltree, element):
        geometry = copy.deepcopy(xmltree)
        for e in geometry:
            e.getparent().remove(e)
        geometry.append(copy.deepcopy(xmltree[0]))
        geometry.append(copy.deepcopy(element))
        return etree.tostring(geometry, encoding="unicode")


class Wfs2Scraper(XmlScraper):

    feature_tag = "{http://www.opengis.net/wfs/2.0}member"

    def make_geometry(self, xmltree, element):
        geometry = copy.deepcopy(xmltree)
        for e in geometry:
            e.getparent().remove(e)
        geometry.append(copy.deepcopy(element))
        return etree.tostring(geometry, encoding="unicode")
