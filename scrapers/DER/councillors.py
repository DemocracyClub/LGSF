from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    disabled = True  # Only returning one councillor
    base_url = "https://cmis.derby.gov.uk/cmis5/Councillors/tabid/62/ScreenMode/Alphabetical/Default.aspx"
