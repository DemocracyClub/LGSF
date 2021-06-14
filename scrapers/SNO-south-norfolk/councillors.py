from lgsf.councillors.scrapers import PagedHTMLCouncillorScraper


class Scraper(PagedHTMLCouncillorScraper):
    base_url = "https://www.south-norfolk.gov.uk/about-us/councillors-and-committees/your-councillors"
    list_page = {
        "container_css_selector": ".listing",
        "councillor_css_selector": ".media",
        "next_page_css_selector": ".pager__item--next",
    }

    def get_next_link(self, soup):
        url = super().get_next_link(soup)
        if url:
            return self.base_url + url

    def get_single_councillor(self, councillor_html):
        url = (
            "https://www.south-norfolk.gov.uk"
            + councillor_html.select(".councillor-sub-details--top")[0].a["href"]
        )
        soup = self.get_page(url)
        name = soup.h1.get_text(strip=True)
        party = soup.find(text="Party").findNext("div").get_text(strip=True)
        division = soup.find(text="Ward").findNext("div").get_text(strip=True)

        # Find a way to call this and return the councillor object
        councillor = self.add_councillor(
            url, identifier=url, name=name, party=party, division=division
        )

        councillor.email = soup.select("a[href^=mailto]")[0].get_text(strip=True)

        councillor.photo_url = (
            "https://www.south-norfolk.gov.uk"
            + soup.find("img", {"class": "image-style-councillor-profile"})["src"]
        )
        return councillor
