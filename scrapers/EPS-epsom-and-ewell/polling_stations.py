from lgsf.polling_stations.scrapers.arcgis_scraper import ArcGisScraper
from lgsf.polling_stations.scrapers.xml_scrapers import GmlScraper


class Scraper(GmlScraper):
    stations_url = "https://maps.epsom-ewell.gov.uk/getOWS.ashx?MapSource=EEBC/inspire&service=WFS&version=1.1.0&request=GetFeature&Typename=pollingstations"
    stations_fields = {
        "{http://mapserver.gis.umn.edu/mapserver}msGeometry": "msGeometry",
        "{http://mapserver.gis.umn.edu/mapserver}psnumber": "psnumber",
        "{http://mapserver.gis.umn.edu/mapserver}district": "district",
        "{http://mapserver.gis.umn.edu/mapserver}address": "address",
        "{http://mapserver.gis.umn.edu/mapserver}ward": "ward",
    }

    districts_url = "https://maps.epsom-ewell.gov.uk/getOWS.ashx?MapSource=EEBC/inspire&service=WFS&version=1.1.0&request=GetFeature&Typename=pollingdistricts"
    districts_fields = {
        "{http://www.opengis.net/gml}boundedBy": "boundedBy",
        "{http://mapserver.gis.umn.edu/mapserver}msGeometry": "msGeometry",
        "{http://mapserver.gis.umn.edu/mapserver}district": "district",
        "{http://mapserver.gis.umn.edu/mapserver}pollingplace": "pollingplace",
    }
    encoding = "utf-8"

    station_key = "psnumber"
    districts_key = "id"
