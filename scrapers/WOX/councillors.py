from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://cmis.westoxon.gov.uk/cmis/Councillors.aspx"
