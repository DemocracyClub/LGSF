from lgsf.polling_stations.scrapers.geojson_scraper import RandomIdGeoJSONScraper
from slugify import slugify


class scraper(RandomIdGeoJSONScraper):
    stations_url = "https://mapping.canterbury.gov.uk/arcgis/rest/services/Open_Data/Polling_Stations/MapServer/0/query?where=OBJECTID+LIKE+%27%25%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&returnIdsOnly=false&returnCountOnly=false&orderByFields=OBJECTID&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=pjson"
    districts_url = "https://mapping.canterbury.gov.uk/arcgis/rest/services/Open_Data/Polling_Boundaries/MapServer/0/query?where=OBJECTID+LIKE+%27%25%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=*&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=4326&returnIdsOnly=false&returnCountOnly=false&orderByFields=OBJECTID&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset=&resultRecordCount=&f=pjson"

    def make_pk(self, feature):
        """
        {... ,
        "features": [{
            "attributes": {
                "OBJECTID": 14921,
                "Ward": "Little Stour & Adisham",
                "Polling_di": "RSA4",
                "GlobalID": "{061DEDF6-7722-4211-A355-8E88E9A73AFD}"
            },
            "geometry": {
                "points": [...]
            }
        },{...}]}
        """
        return "--".join([slugify(x) for x in [feature["Ward"], feature["Polling_di"]]])
