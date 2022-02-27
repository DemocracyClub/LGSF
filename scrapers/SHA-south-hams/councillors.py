import re

from councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.southhams.gov.uk/councillorsSH"
    list_page = {
        "container_css_selector": ".maincontent__left",
        "councillor_css_selector": ".item__content",
    }

    def _get_from_table(self, table, label):
        return (
            table.find("th", text=re.compile(label))
            .find_next("td")
            .get_text(strip=True)
        )

    def get_single_councillor(self, councillor_html):
        url = councillor_html.find("a")["href"]
        councillor_soup = self.get_page(url)
        table = councillor_soup.find("table")
        name = self._get_from_table(table, "Name")
        party = self._get_from_table(table, "Political Party")
        ward = self._get_from_table(table, "Ward")

        councillor = self.add_councillor(
            url,
            identifier=url,
            name=name,
            party=party,
            division=ward,
        )
        councillor.email = councillor_soup.select(".a-body--default a[href^=mailto]")[
            0
        ].get_text(strip=True)
        councillor.photo_url = councillor_soup.select(".a-body--default img")[0]["src"]
        return councillor
