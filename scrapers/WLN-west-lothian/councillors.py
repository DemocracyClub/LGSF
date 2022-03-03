from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://westlothian.gov.uk/councillors"
    list_page = {
        "container_css_selector": ".container .grid--3col",
        "councillor_css_selector": ".grid__cell--listitem",
    }

    def get_councillors(self):
        container = self.get_list_container()
        councillor_soups = []
        for ward in container.select(self.list_page["councillor_css_selector"]):

            councillor_soups += self.get_councillors_from_ward(ward)
        return councillor_soups

    def get_councillors_from_ward(self, ward):
        url = ward.select_one(".item__link")["href"]
        soup = self.get_page(url)
        return soup.select(".grid--list .grid__cell--listitem")

    def get_single_councillor(self, councillor_html):
        url = councillor_html.select_one(".item__link")["href"]
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True).replace("Councillor ", "")
        intro = soup.select_one(".a-intro").get_text(strip=True)

        division = intro.split("is a representative of ")[-1].replace(" ward.", "")
        party = intro.split("(")[-1].split(")")[0]
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)

        councillor.photo_url = soup.select_one(".a-relimage img")["src"]
        return councillor
