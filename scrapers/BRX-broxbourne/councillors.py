from urllib.parse import urljoin
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    disabled = False
    base_url = "https://www.broxbourne.gov.uk/councillors"
    list_page = {
        "container_css_selector": ".container .page-content .list.list--listing",
        "councillor_css_selector": ".list__item",
    }

    def get_single_councillor(self, councillor_html):
        name = councillor_html.h2.get_text(strip=True)
        url = urljoin(self.base_url, councillor_html.findAll("a")[0]["href"])
        identifier = url.split("/")[-2]
        councillor_html = self.get_page(url)
        division = councillor_html.find(text="Ward:").next.strip()
        party = councillor_html.find(text="Party:").next.strip()
        councillor = self.add_councillor(
            url, identifier=identifier, name=name, party=party, division=division
        )
        try:
            councillor.email = (
                councillor_html.find(text="Email:").findNext("a").get_text(strip=True)
            )
        except AttributeError:
            pass

        image_url = councillor_html.find_all("img", class_="image image--feature")[0][
            "src"
        ]
        councillor.photo_url = urljoin(self.base_url, image_url)
        return councillor
