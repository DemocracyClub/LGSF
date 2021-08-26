from lgsf.councillors.scrapers import JSONCouncillorScraper


class Scraper(JSONCouncillorScraper):
    base_url = (
        "https://info.ambervalley.gov.uk/WebServices/AVBCFeeds/DemocracyJSON.asmx"
    )

    def get_councillors(self):
        councillor_list = self.get(f"{self.base_url}/GetAllCouncillors").json()
        return councillor_list

    def get_single_councillor(self, councillor_json):
        memberRef = councillor_json.get("memberRef")
        url = f"{self.base_url}/GetCouncillor?councillorRef={memberRef}"
        single_councillor = self.get(url).json()
        councillor = self.add_councillor(
            url,
            identifier=url,
            name=single_councillor["fullName"],
            party=single_councillor["affiliation"],
            division=single_councillor["ward"],
        )
        councillor.email = single_councillor.get("emailAddress")
        councillor.photo_url = (
            f"{self.base_url}/StreamCouncillorPhoto?memberref={memberRef}"
        )

        return councillor
