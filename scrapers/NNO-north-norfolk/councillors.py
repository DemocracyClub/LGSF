from bs4 import BeautifulSoup
from dateutil.parser import parse

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.north-norfolk.gov.uk/members/#filter-form"
    list_page = {
        "container_css_selector": "#filter-tab-1 select",
        "councillor_css_selector": "option",
    }

    def get_councillors(self):
        return super().get_councillors()[1:]

    def get_single_councillor(self, councillor_html):
        url = "https://www.north-norfolk.gov.uk" + councillor_html["value"]
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        name = soup.h1.get_text(strip=True)
        division = soup.find("h3", text="Ward").findNext("li").get_text(strip=True)
        party = soup.find("h2", text="Group").findNext("p").get_text(strip=True)
        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=division,
        )
        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)
        councillor.photo_url = (
            "https://www.north-norfolk.gov.uk"
            + soup.select(".related-box img")[0]["src"].split("?")[0]
        )

        try:
            next_election = (
                soup.find(text="Next election").findNext("p").get_text(strip=True)
            )
            standing_down = parse(next_election, dayfirst=True)
            councillor.standing_down = standing_down.isoformat()
        except AttributeError:
            pass
        return councillor
