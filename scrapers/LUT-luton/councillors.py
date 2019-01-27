from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://democracy.luton.gov.uk/cmis5public/Councillors.aspx"
