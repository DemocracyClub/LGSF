from lgsf.polling_stations.scrapers.arcgis_scraper import ArcGisScraper


class Scraper(ArcGisScraper):
    stations_url = "https://opendata.arcgis.com/datasets/6b49d2cc9ce44026a3fc232461780c42_18.geojson"
    districts_url = "https://opendata.arcgis.com/datasets/acae4681cabe4ed58d150f3ec9697e25_19.geojson"
