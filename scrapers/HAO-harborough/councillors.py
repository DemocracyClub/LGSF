from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://cmis.harborough.gov.uk/cmis5/Councillors.aspx"
