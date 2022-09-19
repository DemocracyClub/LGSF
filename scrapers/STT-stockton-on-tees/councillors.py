import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.stockton.gov.uk/Find-my-Councillor"

    list_page = {
        "container_css_selector": ".a-body--formsservice",
        "councillor_css_selector": "a",
    }

    def get_single_councillor(self, councillor_html):
        initial_url = urljoin(self.base_url, councillor_html["href"])
        initial_soup = self.get_page(initial_url)

        name = (
            initial_soup.select_one("h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        # For some reason these people don't have a link to the member details pages!
        if name == "David Minchella":
            url = "http://www.egenda.stockton.gov.uk/aksstockton/users/public/admin/main.pl?op=MemberDetails&keyid=708"
        elif name == "Laura Tunney":
            url = "http://www.egenda.stockton.gov.uk/aksstockton/users/public/admin/main.pl?op=MemberDetails&keyid=555"
        elif name == "Lynn Hall":
            url = "http://www.egenda.stockton.gov.uk/aksstockton/users/public/admin/main.pl?op=MemberDetails&keyid=546"
        elif name == "Tina Large":
            url = "http://www.egenda.stockton.gov.uk/aksstockton/users/public/admin/main.pl?op=MemberDetails&keyid=703"
        else:
            url = initial_soup.find("a", href=re.compile("egenda.stockton"))["href"]
        detail_soup = self.get_page(url)

        ward = (
            detail_soup.find("th", text=re.compile("Ward"))
            .find_next("td")
            .get_text(strip=True)
        )

        party = (
            detail_soup.find("th", text=re.compile("Political Group"))
            .find_next("td")
            .get_text(strip=True)
        )
        councillor = self.add_councillor(
            url, name=name, division=ward, party=party, identifier=url
        )
        councillor.email = initial_soup.select("a[href^=mailto]")[0].get_text(
            strip=True)
        councillor.photo_url = urljoin(
            self.base_url, detail_soup.select_one("img.phototgraph")["src"]
        )

        return councillor
