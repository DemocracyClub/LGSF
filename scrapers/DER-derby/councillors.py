from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://democracy.derby.gov.uk/cmis5/Councillors/tabid/62/ScreenMode/Alphabetical/Default.aspx"
    verify_requests = False
