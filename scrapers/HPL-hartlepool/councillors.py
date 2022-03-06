import re

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.hartlepool.gov.uk/councillors/name"

    list_page = {
        "container_css_selector": ".item-list__rich",
        "councillor_css_selector": "li",
    }

    def _get_li_element(self, soup, label):
        li = soup.find("strong", text=re.compile(f"{label}:")).parent
        li.strong.decompose()
        return li.get_text(strip=True)

    def get_single_councillor(self, councillor_html):
        url = councillor_html.a["href"]
        soup = self.get_page(url)
        name = councillor_html.h3.get_text(strip=True)
        ward = self._get_li_element(soup, "Ward")
        party = self._get_li_element(soup, "Party")
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=ward
        )

        councillor.email = soup.select(".callout ul li a[href^=mailto]")[0].get_text(
            strip=True
        )
        councillor.photo_url = f"https:{soup.img['src']}"
        return councillor
