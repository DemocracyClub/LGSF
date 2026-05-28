import contextlib

from bs4 import BeautifulSoup

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "ul.list--listing",
        "councillor_css_selector": "article.listing",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.select_one("a.listing__link")["href"]

        name = councillor_html.select_one("h3.listing__heading span").get_text(
            strip=True
        )

        meta_items = councillor_html.select("li.listing__meta")
        division = meta_items[0].get_text(strip=True).replace("Ward:", "").strip()
        party = meta_items[1].get_text(strip=True).replace("Party:", "").strip()

        identifier = url.split("/councillors/")[1].split("/")[0]

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )

        with contextlib.suppress(AttributeError, TypeError):
            img = councillor_html.select_one("img.listing__image")
            if img:
                img_src = img.get("src", "")
                if img_src and not img_src.startswith("data:"):
                    if img_src.startswith("//"):
                        img_src = "https:" + img_src
                    councillor.photo_url = img_src

        with contextlib.suppress(AttributeError, IndexError, TypeError):
            profile_soup = BeautifulSoup(self.get_text(url), "lxml")
            email_link = profile_soup.select_one("a[href^=mailto]")
            if email_link:
                councillor.email = email_link["href"].replace("mailto:", "")

        return councillor
