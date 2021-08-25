from councillors.exceptions import SkipCouncillorException
from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    """
    Adur and Worthing are two councils that share a single management team and
    website. This means that we have a single MG install for two sets of
    councilors.

    Deal with this by just looking at the email address domain to tell what
    councils they're in.

    Not ideal, but it's a good enough first attempt.
    """

    base_url = "https://democracy.adur-worthing.gov.uk"

    def get_single_councillor(self, ward, councillor_xml):
        councillor = super().get_single_councillor(ward, councillor_xml)
        email = getattr(councillor, "email", None)
        if email and "worthing.gov.uk" in email:
            return councillor
        else:
            raise SkipCouncillorException()
