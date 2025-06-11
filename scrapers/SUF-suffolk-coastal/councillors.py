from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://eastsuffolk.cmis.uk.com/eastsuffolk/councillors.aspx"

    def get_party_name(self, list_page_html):
        return (
            list_page_html.find_all("img")[-1]["alt"]
            .replace("(logo)", "")
            .strip()
        )
