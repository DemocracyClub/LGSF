from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    list_page = {
        "container_css_selector": ".p-md-3",
        "councillor_css_selector": ".councillor-item",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.a["href"]
        soup = self.get_page(url)
        name_el = soup.select_one("h1")
        if name_el:
            name = name_el.get_text(strip=True)
        else:
            name = (
                soup.select_one("div.col-12.col-md-8")
                .find_next("p")
                .get_text(strip=True)
            )
        party_el = councillor_html.strong
        party = party_el.get_text(strip=True)
        division = party_el.find_next("p").get_text(strip=True)

        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )
        email_link = soup.select_one("a[href^=mailto]")
        if not email_link:
            email_link = soup.select_one("#S4_EmailPlaceholder")
        if email_link:
            councillor.email = email_link.get_text(strip=True)
        councillor.photo_url = councillor_html.select_one("img")["src"]
        return councillor
