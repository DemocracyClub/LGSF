from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.threerivers.gov.uk/listing/councillors"
    list_page = {
        "container_css_selector": "div.councillor:nth-child(2) > ul:nth-child(2)",
        "councillor_css_selector": "li",
    }

    def get_list_container(self):
        soup = self.get_page(self.base_url)
        return soup.find("h3", text="District Councillor").findNext("ul")

    def get_single_councillor(self, councillor_html):
        url = councillor_html.a["href"]
        soup = self.get_page(url)
        name = councillor_html.get_text(strip=True)
        party = soup.find(text="Party:").next.get_text(strip=True)
        division = (
            soup.find(text="Area of representation:")
            .next.get_text(strip=True)
            .replace("Ward - ", "")
        )
        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(
            strip=True
        )

        councillor.photo_url = "https://www.threerivers.gov.uk" + soup.find("p", {"class": "image"}).img["src"]
        return councillor
