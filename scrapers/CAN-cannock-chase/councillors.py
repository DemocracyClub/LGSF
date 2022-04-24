import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = (
        "https://www.cannockchasedc.gov.uk/council/about-council/your-councillors"
    )

    list_page = {
        "container_css_selector": "article.node-councillor-landing-page",
        "councillor_css_selector": ".councillor_more_med",
    }

    def get_single_councillor(self, councillor_html):
        link = councillor_html.find("a", href=re.compile("/council/"))
        url = urljoin(self.base_url, link["href"])

        soup = self.get_page(url)

        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        party = soup.select_one(".councillor_party").get_text(strip=True)
        division = (
            soup.find("div", text=re.compile("Ward:"))
            .find_next("div", {"class": "desc"})
            .get_text(strip=True)
        )
        print(name, party)
        print(name, division)

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )
        councillor.email = soup.select_one("div.email").getText(strip=True)
        councillor.photo_url = soup.select_one("div.councillor_image img")["src"]
        return councillor
