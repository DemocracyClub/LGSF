from urllib.parse import urljoin

from councillors.scrapers import JSONCouncillorScraper


class Scraper(JSONCouncillorScraper):
    base_url = "https://www.redcar-cleveland.gov.uk/about-the-council/councillors/_api/web/lists/getbytitle('Councillors%20List')/items?$orderby=Title"

    def get_councillors(self):
        councillor_list = self.get(
            self.base_url, extra_headers={"Accept": "application/json"}
        ).json()["value"]
        return councillor_list

    def get_single_councillor(self, councillor_json):
        councillor_id = str(councillor_json.get("Id"))
        name = councillor_json["Title"]
        url = f"https://www.redcar-cleveland.gov.uk/about-the-council/councillors/Pages/Your-Councillor.aspx?search=Name&val={name}"
        councillor = self.add_councillor(
            url,
            identifier=councillor_id,
            name=name,
            party=councillor_json["Party"],
            division=councillor_json["Ward"],
        )
        councillor.email = councillor_json.get("Email")
        councillor.photo_url = urljoin(self.base_url, councillor_json["Photo"])

        return councillor
