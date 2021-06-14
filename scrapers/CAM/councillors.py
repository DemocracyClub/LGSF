from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://cmis.cambridgeshire.gov.uk/ccc_live/Councillors.aspx"
    division_text = "Division:"
