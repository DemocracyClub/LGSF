from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://cmis.daventrydc.gov.uk/daventry/Councillors/tabid/63/ScreenMode/Alphabetical/Default.aspx"
