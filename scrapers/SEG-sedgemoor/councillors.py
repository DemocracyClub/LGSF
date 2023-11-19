from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.sedgemoor.gov.uk/councillors"
    list_page = {
        "container_css_selector": ".content__wrapper--withmenu",
        "councillor_css_selector": ".contact-list__wrapper",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.h3.a["href"]
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)
        division = (
            soup.find(text="Ward:")
            .findNext("div")
            .get_text(strip=True)
            .split("(")[0]
            .strip()
        )
        party = (
            soup.find(text="Political Party:")
            .findNext("div")
            .get_text(strip=True)
            .split("(")[0]
            .strip()
        )
        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(
            strip=True
        )

        councillor.photo_url = soup.find("div", {"class": "imagelink"}).img[
            "src"
        ]
        return councillor
