from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    def get_single_councillor(self, ward, councillor_xml):
        councillor = super().get_single_councillor(ward, councillor_xml)
        if hasattr(councillor, "email") and "southhams.gov.uk" in councillor.email:
            return councillor
        raise SkipCouncillorException()
