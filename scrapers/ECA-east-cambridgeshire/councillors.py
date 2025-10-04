from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".views-fluid-grid",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        url = urljoin(self.base_url, councillor_html.a["href"])
        soup = self.get_page(url).select_one("#content")
        name = soup.h2.get_text(strip=True).replace("Councillor ", "")
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
