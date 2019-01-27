from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://fylde.cmis.uk.com/fylde/CouncillorsandMP.aspx"
