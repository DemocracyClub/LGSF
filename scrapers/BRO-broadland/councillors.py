from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    def get_single_councillor(self, ward, councillor_xml):
        councillor = super().get_single_councillor(ward, councillor_xml)
        if "(BDC)" in councillor.division:
            return councillor
        raise SkipCouncillorException()
