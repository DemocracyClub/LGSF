import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".view-councillors",
        "councillor_css_selector": "h2",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("main")

        name = councillor_html.get_text(strip=True).replace("Councillor ", "")
        division = (
            soup.find("div", text=re.compile("Ward representation"))
            .find_next("div", {"class": "field__item"})
            .get_text(strip=True)
        )
        party = (
            soup.find("div", text=re.compile("Party"))
            .find_next("div", {"class": "field__item"})
            .get_text(strip=True)
        )
        councillor = self.add_councillor(
            url, identifier=url, party=party, division=division, name=name
        )

        email_el = soup.find("div", text=re.compile("Email"))
        if email_el:
            councillor.email = email_el.find_next("div").get_text(strip=True)

        # Photo - find the councillor photo, not accessibility icons
        photo_imgs = soup.select("img")
        for img in photo_imgs:
            src = img.get("src", "")
            alt = img.get("alt", "")
            # Skip accessibility icons and other non-photo images
            if not any(
                x in src.lower() for x in ["accessibility", "icon", "logo", "recite"]
            ) and ("cllr" in alt.lower() or "councillor" in alt.lower()):
                councillor.photo_url = urljoin(self.base_url, src)
                break

        return councillor
