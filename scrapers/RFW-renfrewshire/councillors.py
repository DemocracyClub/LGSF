from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    timeout = 20
    base_url = "https://renfrewshire.cmis.uk.com/renfrewshire/Councillors.aspx"
