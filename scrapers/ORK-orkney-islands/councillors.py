import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    verify_requests = False
    list_page = {
        "container_css_selector": "main",
        # Each card is rendered twice, once for desktop and once for mobile
        "councillor_css_selector": "div.hidden.md\\:block",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(
            self.base_url,
            councillor_html.select_one("a[href*='find-your-councillor/']")["href"],
        )
        soup = self.get_page(url).select_one("main")

        name = soup.select_one("h1").get_text(strip=True).replace("Councillor ", "")

        ward = (
            soup.find("p", string=re.compile("Ward"))
            .find_next_sibling("p")
            .get_text(strip=True)
        )

        # There are not many parties who stand! So they don't actually list
        # them on the website. Because of that, we have to do this...
        if "Orkney Manifesto Group" in soup.get_text():
            party = "Orkney Manifesto Group"
        elif "Scottish Green Party" in soup.get_text():
            party = "Scottish Green Party"
        else:
            party = "Independent"

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        email = soup.select_one("a[href^=mailto]")
        if email:
            councillor.email = email["href"].replace("mailto:", "")
        image = soup.select_one("img[src*=Councillors]")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
