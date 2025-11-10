import re
from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": "main",
        "councillor_css_selector": "article",
    }

    def _get_paragraph_with_label(self, soup, label):
        """Extract text from paragraph with strong label"""
        para = soup.find("strong", text=re.compile(f"{label}:"))
        if para:
            para = para.parent
            # Get text after the strong tag
            return para.get_text(strip=True).replace(f"{label}:", "").strip()
        return None

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("main")
        name = councillor_html.h2.get_text(strip=True)
        ward = self._get_paragraph_with_label(soup, "Ward")
        party = self._get_paragraph_with_label(soup, "Party")
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=ward
        )

        # Email
        # Photo
        email_link = soup.select_one(".page-content-inner a[href^=mailto]")
        if email_link:
            councillor.email = email_link["href"].replace("mailto:", "")

        photo_img = soup.select_one(".page-content-inner img")
        if photo_img and photo_img.get("src"):
            councillor.photo_url = urljoin(self.base_url, photo_img["src"])

        return councillor
