import re

from lgsf.councillors import SkipCouncillorException
from lgsf.councillors.scrapers import HTMLCouncillorScraper


class Scraper(HTMLCouncillorScraper):
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
        if name == "Vacant":
            raise SkipCouncillorException
        intro = soup.select_one(".a-intro").get_text(strip=True)

        division = intro.split("is a representative of ")[-1].replace(" ward.", "")
        party = intro.split("(")[-1].split(")")[0]
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        # Try to find email - scope to main content area to avoid spurious matches
        # Use .a-body--default to skip cookie notice which also has .a-body class
        content_area = soup.select_one(".a-body--default") or soup
        mailto_links = content_area.select("a[href^=mailto]")
        if mailto_links:
            # Extract from href attribute instead of text to avoid "(opens new window)" etc
            href = mailto_links[0].get("href", "")
            councillor.email = href.replace("mailto:", "").split("?")[0]
        else:
            # Search for email pattern in the page text
            page_text = content_area.get_text()
            # Use a more precise regex that stops at word boundaries like "Tel" after "gov.uk"
            email_match = re.search(
                r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}?(?=[A-Z][a-z]|[^A-Za-z0-9.@]|$)",
                page_text,
            )
            if email_match:
                councillor.email = email_match.group(0)

        # Try to find photo URL
        photo_img = soup.select_one(".a-relimage img")
        if photo_img:
            councillor.photo_url = photo_img["src"]

        return councillor
