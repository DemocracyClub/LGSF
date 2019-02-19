from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.runnymede.gov.uk/article/15001/Councillors"
    list_page = {
        "container_css_selector": ".grid--list",
        "councillor_css_selector": ".grid__cell--listitem",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.a["href"]
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)
        division = soup.h2.get_text(strip=True).split(":")[-1].strip()
        party = soup.select(".a-body p")[2].get_text().split(":")[-1].strip()

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        councillor.email = soup.select("a[href^=mailto]")[0].get_text(
            strip=True
        )
        councillor.photo_url = soup.select(".a-relimage img")[0]["src"]
        return councillor
