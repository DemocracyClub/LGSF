from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://cmis.northamptonshire.gov.uk/cmis5live/Councillors.aspx"

    