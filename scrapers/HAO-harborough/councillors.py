from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    verify_requests = False
    base_url = "https://cmis.harborough.gov.uk/cmis5/Councillors.aspx"
