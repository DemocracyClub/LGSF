from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://braintree.cmis.uk.com/braintree/Councillors.aspx"
