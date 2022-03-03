import re

from councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
    base_url = "https://www.nelincs.gov.uk/your-council/councillors-mps-and-meps/find-your-councillor/councillors-by-party/"

    list_page = {
        "container_css_selector": ".wp-block-column",
        "councillor_css_selector": "li",
    }

    def get_single_councillor(self, councillor_html):
        url = councillor_html.a["href"]
        page = self.get_page(url)
        soup = page.select(".wp-block-column")[0]
        party_guess = soup.find_all("p")[1].get_text(strip=True)
        title = soup.h2.get_text(strip=True)
        assert title.startswith(party_guess)
        name = soup.p.get_text(strip=True)
        party = party_guess
        ward = soup.find("a", href=re.compile("ward")).get_text(strip=True)
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=ward
        )
        councillor.email = (
            page.select(".wp-block-column")[1]
            .find(lambda t: t.name == "p" and "Email" in t.text)
            .a["href"]
        ).replace("mailto:", "")
        councillor.photo_url = soup.img["src"]
        return councillor
