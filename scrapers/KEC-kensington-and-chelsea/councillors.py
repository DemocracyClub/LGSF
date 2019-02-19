from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.rbkc.gov.uk/council-and-democracy/councillors-and-committees/councillors/cllrs-ward/councillors-and-their-wards"
    list_page = {
        "container_css_selector": "table",
        "councillor_css_selector": "tr",
    }

    def get_councillors(self):
        return super().get_councillors()[1:]

    def get_single_councillor(self, councillor_html):
        url = "https://www.rbkc.gov.uk" + councillor_html.a['href']
        soup = self.get_page(url)
        councillor_table = soup.select(".organisationDetails")[0]
        name = soup.h1.get_text(strip=True)
        print(name)
        import ipdb; ipdb.set_trace()
        raise NotImplementedError
        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = councillor_table.select("a[href^=mailto]")[0].get_text(
            strip=True
        )
