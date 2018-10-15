from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "http://cmis.carlisle.gov.uk/cmis/CarlisleCityCouncillors/tabid/62/ScreenMode/Alphabetical/Default.aspx"
