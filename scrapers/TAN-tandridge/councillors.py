from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://www.councillors.tandridge.gov.uk/cmis5/Councillors/tabid/62/ScreenMode/Alphabetical/Default.aspx"
