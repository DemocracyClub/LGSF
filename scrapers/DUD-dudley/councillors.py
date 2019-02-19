from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://cmis.dudley.gov.uk/cmis5/Councillors.aspx"

    