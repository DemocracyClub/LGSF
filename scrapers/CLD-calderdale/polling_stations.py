from lgsf.polling_stations.scrapers.geojson_scraper import GeoJsonScraper


class Scraper(GeoJsonScraper):
    stations_url = "https://dataworks.calderdale.gov.uk/download/polling-stations/f9b00312-f330-4d18-a944-7cfd36c8d0eb/Polling%20stations.geojson"
    districts_url = "https://dataworks.calderdale.gov.uk/download/polling-station-districts/2482d9f3-7eae-4ea6-980c-718e9723e64a/Polling%20districts.geojson"
    key = "POLLING_LETTERS"
    encoding = "utf-8"
