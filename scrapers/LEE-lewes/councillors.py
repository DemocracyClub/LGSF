from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "http://democracy.lewes-eastbourne.gov.uk/"

    def get_single_councillor(self, ward, councillor_xml):
        councillor = super().get_single_councillor(ward, councillor_xml)
        email = getattr(councillor, "email", None)
        if councillor.identifier == "349":
            return councillor
        if "lewes.gov.uk" in email:
            return councillor
        else:
            raise SkipCouncillorException()
