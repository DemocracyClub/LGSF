from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    disabled = True
    base_url = "http://www.ceredigion.gov.uk/your-council/councillors-committees/councillors/"
    list_page = {
        "container_css_selector": ".councillors",
        "councillor_css_selector": ".councillor-box",
    }

    def get_single_councillor(self, councillor_html):
        print(councillor_html)
        import ipdb; ipdb.set_trace()
        raise NotImplementedError
        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
