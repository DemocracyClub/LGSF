import re

from bs4 import BeautifulSoup

from lgsf.scrapers.councillors import CMISCouncillorScraper


class Scraper(CMISCouncillorScraper):
    base_url = "https://birmingham.cmis.uk.com/birmingham/Councillors.aspx"

    def get_from_profile_page(self, profile_url):
        # Get the real profile page
        req = self.get(profile_url)
        soup = BeautifulSoup(req.text, "lxml")

        identifier = profile_url.split("/councillors/")[1].split("/")[0]
        name = soup.find("h2", {"class": "listing__heading"}).getText(
            strip=True
        )
        division = soup.find(text="Ward:").next.strip()
        party = soup.find(text="Party:").next.strip()

        councillor = self.add_councillor(
            profile_url,
            identifier=identifier,
            name=name,
            party=party,
            division=division,
        )
        councillor.email = soup.find(text=re.compile("Email:")).next.getText(
            strip=True
        )

        return councillor

    def get_single_councillor(self, list_page_html):
        """
        Birmingham uses CMIS for councillors, but another URL for profiles
        and contact info. This method traverses cmis to that profile URL and
        parses it in to a Councillor object
        """
        url = list_page_html.a["href"]

        # Get the CMIS page for this person
        req = self.get(url)
        soup = BeautifulSoup(req.text, "lxml")
        try:
            profile_url = soup.select(".Biog")[0].a["href"].strip()
            councillor = self.get_from_profile_page(profile_url)
        except:
            # This person doesn't have a profile page or something else went
            # wrong, do what we can with this page
            councillor = super().get_single_councillor(list_page_html)
        return councillor
