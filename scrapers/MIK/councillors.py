from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://milton-keynes.cmis.uk.com/milton-keynes/Councillors.aspx"
