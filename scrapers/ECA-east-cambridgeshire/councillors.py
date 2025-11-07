from urllib.parse import urljoin

from lgsf.councillors.scrapers import PagedHTMLCouncillorScraper


class Scraper(PagedHTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".localgov-directory__content",
        "councillor_css_selector": ".view-content article",
        "next_page_css_selector": "a[rel=next]",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("main")
        name = soup.h1.get_text(strip=True).replace("Cllr ", "")
        division = (
            soup.select_one(".views-field-field-ward")
            .get_text(strip=True)
            .replace("Ward:", "")
        )
        party = (
            soup.select_one(".views-field-field-party")
            .get_text(strip=True)
            .replace("Party:", "")
        )
        councillor = self.add_councillor(
            url, identifier=url, name=name, division=division, party=party
        )
        email = soup.select_one(".views-field-field-email-link")
        if email:
            if email.a:
                email = email.a["href"]
                email = email.split("=")[-1].replace("%40", "@")
            else:
                email = email.get_text(strip=True).replace("Email:", "")
            councillor.email = email
        photo_element = soup.select_one(".views-field-field-image img")
        if photo_element:
            councillor.photo_url = urljoin(self.base_url, photo_element["src"])
        return councillor
