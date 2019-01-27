from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://democracy.bolton.gov.uk/cmis5/People.aspx"
