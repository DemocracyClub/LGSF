from slugify import slugify

from lgsf.scrapers.councillors import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "http://www.shetland.gov.uk/about_your_councillors/"
    list_page = {
        "container_css_selector": "#content",
        "councillor_css_selector": "tr",
    }

    def get_councillors(self):
        container = self.get_list_container()
        return container.select(self.list_page["councillor_css_selector"])[1:]

    def get_single_councillor(self, councillor_html):
        url = self.base_url
        info_cell = councillor_html.findAll("td")[1]
        name = info_cell.findAll("p")[0].get_text(strip=True)
        division = info_cell.findAll("p")[1].get_text(strip=True)
        print(division)
        party = "Independent"
        identifier = "--".join([slugify(x) for x in [name, division]])

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )

        councillor.email = councillor_html.a.get_text(strip=True)
        councillor.photo_url = self.base_url + councillor_html.img['src']

        return councillor
