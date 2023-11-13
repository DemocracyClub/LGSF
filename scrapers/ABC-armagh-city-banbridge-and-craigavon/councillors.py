import codecs
import re

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.armaghbanbridgecraigavon.gov.uk/councillors/"
    list_page = {
        "container_css_selector": ".td-main-content",
        "councillor_css_selector": ".council-wrap-inner",
    }

    def decode_email(self, councillor_html):
        borked_email = councillor_html.select_one("a.mailto-link")["data-enc-email"]
        email = borked_email.replace("[at]", "@")
        return codecs.encode(email, "rot_13")

    def get_single_councillor(self, councillor_html):
        header = councillor_html.h3
        name = header.get_text(strip=True)
        party = header.find_next("p").get_text(strip=True)
        division = (
            councillor_html.find(text=re.compile("District:"))
            .get_text(strip=True)
            .replace("District:", "")
            .strip()
        )
        identifier = header.a["href"]
        email = self.decode_email(councillor_html)

        councillor = self.add_councillor(
            self.base_url,  # Not ideal
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        councillor.email = email

        councillor.photo_url = councillor_html.select_one("img")["src"]
        councillor.photo_url = councillor.photo_url.replace("120x150", "240x300")
        return councillor
