from lgsf.polling_stations.scrapers.arcgis_scraper import ArcGisScraper


class Scraper(ArcGisScraper):

    # search  _url = "https://opendata.arcgis.com/api/v3/datasets?q=polling&filter%5Bowner%5D=Broxtowe_GIS&fields[datasets]=name,url"
    stations_url = "https://opendata.arcgis.com/datasets/a3da8ff112a647fe857a45c9996488fd_3.geojson"
    districts_url = "https://opendata.arcgis.com/datasets/ace0ecbc348a41b5b804557f6a68f575_1.geojson"
