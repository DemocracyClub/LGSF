import re
from urllib.parse import urljoin

from slugify import slugify

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.newrymournedown.org/your-councillors"

    list_page = {
        "container_css_selector": ".list_images_text",
        "councillor_css_selector": "li",
    }

    raw_html = None

    def get_raw_html(self):
        if not self.raw_html:
            self.raw_html = self.get_page(self.base_url)
        return self.raw_html

    def get_ward_for_person(self, name):
        raw_html = self.get_raw_html()
        title_tag = raw_html.find(string=re.compile(name))
        ward = title_tag.find_all_previous("h1")[0].get_text(strip=True)
        return ward.replace(" Councillors", "").strip()

    def get_single_councillor(self, councillor_html):
        if not councillor_html.select_one(".lmt_content"):
            raise SkipCouncillorException()
        raw_name = councillor_html.select_one("span.name").get_text(strip=True)
        name = " ".join(reversed(raw_name.split(","))).strip()

        party = (
            councillor_html.find("strong", text=re.compile("Party:"))
            .find_next("span")
            .get_text(strip=True)
        )
        division = self.get_ward_for_person(raw_name)

        councillor_id = slugify(f"{name} {party}")

        councillor = self.add_councillor(
            self.base_url,
            identifier=councillor_id,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = councillor_html.select_one("a[href^=mailto]")[
            "href"
        ][6:]
        councillor.photo_url = urljoin(
            self.base_url, councillor_html.img["src"]
        )
        return councillor
