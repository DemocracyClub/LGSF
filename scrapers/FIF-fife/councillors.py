import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.fife.gov.uk/kb/docs/articles/about-your-council2/politicians-and-committees/your-local-councillors/councillor"

    list_page = {
        "container_css_selector": ".article-body",
        "councillor_css_selector": ".councillor-card",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("h1.page-title").get_text(strip=True).replace("Cllr. ", "")
        )
        ward = (
            soup.find("b", text=re.compile("Ward:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )
        party = (
            soup.find("b", text=re.compile("Party:"))
            .find_parent("p")
            .get_text(strip=True)
            .replace("Party:", "")
            .strip()
        )

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )

        councillor.email = soup.select_one(".councillor-social a[href^=mailto]")[
            "href"
        ].replace("mailto:", "")
        councillor.photo_url = urljoin(
            self.base_url,
            soup.select_one(".asset-contents img")["src"],
        )
        return councillor
