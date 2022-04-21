from lgsf.councillors.scrapers import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    # https://github.com/DemocracyClub/LGSF/issues/80
    disabled = True
    base_url = "http://democracy.bolton.gov.uk/cmis5/People.aspx"
