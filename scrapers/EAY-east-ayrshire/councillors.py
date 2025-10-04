import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    disabled = False
    list_page = {
        "container_css_selector": ".main-article",
        "councillor_css_selector": ".col-md-3",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.select_one("a")["href"])
        soup = self.get_page(url)
        name = councillor_html.select_one("a").get_text(strip=True)
        content_box = (
            soup.select_one("article.councillor-profile")
            .find_parent("div")
            .get_text(strip=True, separator="\n")
            .splitlines()
        )
        party = content_box[2]
        division = soup.h2.get_text(strip=True).split(":")[-1].strip()
        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )
        councillor.email = soup.select_one("a[href^=mailto]")["href"]
        councillor.photo_url = urljoin(
            self.base_url,
            soup.find("img", src=re.compile("Councillors"))["src"],
        )

        return councillor
