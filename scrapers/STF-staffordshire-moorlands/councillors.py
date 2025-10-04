from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    def get_single_councillor(self, ward, councillor_xml):
        councillor = super().get_single_councillor(ward, councillor_xml)
        if not getattr(councillor, "email", None):
            if councillor.identifier == "1414":
                # This person doesn't have an email address listed
                return councillor
        if "staffsmoorlands.gov.uk" in councillor.email:
            return councillor
        raise SkipCouncillorException()
