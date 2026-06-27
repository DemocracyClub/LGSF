from urllib.parse import urljoin

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    # Site migrated from /councillors/name to /council-and-democracy/councillors
    # (LocalGov Drupal, Big Blue Door). Councillors are grouped by ward in
    # accordion panes; each .card div holds all list-page data.
    list_page = {
        "container_css_selector": ".view-id-councillors .view-content",
        "councillor_css_selector": ".card",
    }

    def get_single_councillor(self, councillor_html):
        link = councillor_html.select_one(".views-field-title a")
        url = urljoin(self.base_url, link["href"])
        name = link.get_text(strip=True)

        ward = councillor_html.select_one(
            ".views-field-field-ward .field-content"
        ).get_text(strip=True)

        party = councillor_html.select_one(
            ".views-field-field-party .field-content"
        ).get_text(strip=True)

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )

        img = councillor_html.select_one(".views-field-field-image img")
        if img:
            img_src = img.get("data-src") or img.get("src")
            if img_src and not img_src.startswith("data:"):
                councillor.photo_url = urljoin(self.base_url, img_src)

        detail = self.get_page(url)
        email_link = detail.select_one("a[href^='mailto:']")
        if email_link:
            councillor.email = email_link.get_text(strip=True)

        return councillor
