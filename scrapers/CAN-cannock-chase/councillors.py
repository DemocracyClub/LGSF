import re
from urllib.parse import urljoin

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".all-councillors",
        "councillor_css_selector": ".all-container",
    }

    def get_single_councillor(self, councillor_html):
        link = councillor_html.find("a")
        if not link:
            vacant = councillor_html.find("a", href=re.compile("/vacant-seat"))
            if vacant:
                raise SkipCouncillorException("Vacancy")
        url = urljoin(self.base_url, link["href"].strip())

        soup = self.get_page(url)

        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        party = soup.find(text="Party:").find_next("p").get_text(strip=True)
        division = soup.find(text="Ward:").find_next("p").get_text(strip=True)

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )
        try:
            councillor.email = soup.select("a[href^=mailto]")[0]["href"].split(":")[1]

        except IndexError:
            pass
        councillor.photo_url = soup.select_one("div.council-img img")["src"]
        return councillor
