from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://cmis.essex.gov.uk/EssexCmis5/Councillors.aspx"
    division_text = "Division:"
