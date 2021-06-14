import re
from urllib.parse import unquote

from bs4 import BeautifulSoup
from slugify import slugify

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.armaghbanbridgecraigavon.gov.uk/team-showcase/councillors/"
    list_page = {
        "container_css_selector": ".wmts_members",
        "councillor_css_selector": ".wmts_member",
    }

    def decode_email(self, councillor_html):
        js_string = councillor_html.noscript.previous
        mi = re.search('mi="([^"]+)', js_string).group(1)
        ml = re.search('ml="([^"]+)', js_string).group(1)

        ret = []
        for i, char in enumerate(mi):
            try:
                ret.append(ml[ord(char) - 48])
            except ValueError:
                pass
        html_string = unquote("".join(ret))
        soup = BeautifulSoup(html_string, "lxml")
        return soup.a["href"].split(":")[1]

    def get_single_councillor(self, councillor_html):
        name = councillor_html.h2.get_text(strip=True)
        party = councillor_html.h3.get_text(strip=True)
        division = councillor_html.find(text="District:").next_element.get_text(
            strip=True
        )
        identifier = "--".join([slugify(x) for x in [name, party, division]])
        email = self.decode_email(councillor_html)

        councillor = self.add_councillor(
            self.base_url,  # Not ideal
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        councillor.email = email
        councillor.photo_url = councillor_html.findAll("img")[1]["data-wmts-lazysrc"]
        return councillor
