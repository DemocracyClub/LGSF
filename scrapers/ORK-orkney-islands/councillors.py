import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.orkney.gov.uk/Council/Councillors/councillors.htm"

    list_page = {
        "container_css_selector": ".contentWrapper",
        "councillor_css_selector": "td:first-child",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url)

        name = (
            soup.select_one("h1")
            .get_text(strip=True)
            .replace("Councillor ", "")
        )

        ward = (
            soup.find(text=re.compile("Ward:"))
            .get_text(strip=True)
            .replace("Ward:", "")
            .strip()
        )

        # There are not many parties who stand! So they don't actually list
        # them on the website. Beacuse of that, we have to do this...
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
        try:
            councillor.email = soup.select_one("a[href^=mailto]")[
                "href"
            ].replace("mailto:", "")
        except TypeError:
            councillor.email = (
                soup.find(text=re.compile("Email:"))
                .get_text(strip=True)
                .replace("Email:", "")
                .strip()
            )
        image = soup.select_one("img#pic1")
        if image:
            councillor.photo_url = urljoin(
                self.base_url,
                image["src"],
            )
        return councillor
