from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.southtyneside.gov.uk"

    def get_redirect_url_from_page(self, soup):
        pass

    def get_councillors(self):
        self.get_page(f"{self.base_url}/article/60208/Find-your-councillors")
        import ipdb

        ipdb.set_trace()

    def get_single_councillor(self):
        pass
