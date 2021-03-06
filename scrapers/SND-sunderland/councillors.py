from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://www.sunderland.gov.uk/committees/cmis5/Members.aspx"

    def get_party_name(self, list_page_html):
        return (
            list_page_html.find_all("img")[-1]["alt"]
            .replace("(logo)", "")
            .strip()
        )
