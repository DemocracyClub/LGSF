import re

from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.castlepoint.gov.uk/councillors"
    list_page = {
        "container_css_selector": "main.main-article-area-7",
        "councillor_css_selector": ".main-page-pod-link",
    }

    def get_single_councillor(self, councillor_html):
        url = f"https://www.castlepoint.gov.uk{councillor_html['href']}"
        soup = self.get_page(url)
        name = soup.find(text=re.compile("Councillor:")).next.get_text(
            strip=True
        )
        name = " ".join(name.split(" ")[1::-1]).replace(",", "")
        party = soup.find(text=re.compile("Party:")).next.get_text(strip=True)
        division = soup.find(text=re.compile("Ward:")).next.get_text(strip=True)
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        emails = [
            e.get_text(strip=True)
            for e in soup.select("a[href^=mailto]")
            if "@" in e.get_text(strip=True)
            and e.get_text(strip=True) != "info@castlepoint.gov.uk"
        ]
        if len(emails) == 1:
            councillor.email = emails[0]
        try:
            councillor.photo_url = (
                "https://www.castlepoint.gov.uk"
                + soup.find(text=re.compile("Councillor:"))
                .find_parent("div", {"class": "main-editor-output"})
                .find("img")["src"]
            )
            print(councillor.photo_url)
        except AttributeError:
            pass

        return councillor
