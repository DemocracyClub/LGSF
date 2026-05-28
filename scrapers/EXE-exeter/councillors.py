import contextlib
from urllib.parse import urljoin, urlparse, parse_qs

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".mgThumbsList",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        link = councillor_html.select_one("a")
        url = urljoin(self.base_url, link["href"])

        qs = parse_qs(urlparse(url).query)
        identifier = qs.get("UID", [url])[0]

        name = link.get_text(strip=True).replace("Councillor ", "").strip()

        paragraphs = councillor_html.select("p")
        division = paragraphs[0].get_text(strip=True) if paragraphs else ""
        party = paragraphs[1].get_text(strip=True) if len(paragraphs) > 1 else ""

        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )

        with contextlib.suppress(AttributeError, TypeError):
            img = councillor_html.select_one("img")
            if img:
                img_src = img.get("src")
                if img_src and not img_src.startswith("data:"):
                    councillor.photo_url = urljoin(self.base_url, img_src)

        with contextlib.suppress(AttributeError, IndexError, TypeError):
            detail_soup = self.get_page(url)
            email_link = detail_soup.select_one("a[href^=mailto]")
            if email_link:
                councillor.email = email_link["href"].replace("mailto:", "")

        return councillor
