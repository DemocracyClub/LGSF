from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://colchester.cmis.uk.com/colchester/Councillors.aspx"
