from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://rochford.cmis.uk.com/rochford/Members/tabid/62/ScreenMode/Alphabetical/Default.aspx"
