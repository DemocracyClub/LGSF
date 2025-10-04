from lgsf.councillors.scrapers import BaseCouncillorScraper


class Scraper(BaseCouncillorScraper):
    disabled = True

    def get_councillors(self):
        """Placeholder implementation for disabled scraper."""
        return []

    def get_single_councillor(self, councillor_html):
        """Placeholder implementation for disabled scraper."""
        pass
