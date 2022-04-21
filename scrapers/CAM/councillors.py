from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://cambridgeshire.cmis.uk.com/ccc_live/Councillors.aspx"
    division_text = "Division:"
