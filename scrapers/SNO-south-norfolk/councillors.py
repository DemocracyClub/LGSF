from lgsf.councillors import CouncillorBase
from lgsf.councillors.scrapers import ModGovCouncillorScraper


class Scraper(ModGovCouncillorScraper):
    base_url = "https://southnorfolkandbroadland.moderngov.co.uk"

    def exclude_councillor_hook(self, councillor: CouncillorBase):
        if "(SNC)" not in councillor.division:
            return True
        return None
