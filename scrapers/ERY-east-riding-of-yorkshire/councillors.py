from lgsf.councillors.scrapers import JSONCouncillorScraper

ENTRY_API_URL = "https://www.eastriding.gov.uk/index.php?option=com_erycdirectory&task=entrydata.fetchentrybyalias&format=json&directory_id=43&alias={alias}"


class Scraper(JSONCouncillorScraper):
    def get_councillors(self):
        """Override to return JSON entries directly"""
        json_data = self.get(self.base_url).json()
        return json_data.get("searchEntries", [])

    def get_single_councillor(self, councillor_json):
        """Parse a single councillor from JSON"""
        search_data = councillor_json.get("search_data", {})

        # Extract name from nested structure
        name_data = search_data.get("name", [{}])[0]
        first_name = name_data.get("firstName", "")
        last_name = name_data.get("lastName", "")
        name = f"{first_name} {last_name}".strip()

        # Extract ward (division)
        division = search_data.get("ward", "")

        # Extract party (use political_group as fallback)
        party = search_data.get("party", "") or search_data.get("political_group", "")

        # Build URL from alias
        alias = councillor_json.get("alias", "")
        url = f"https://www.eastriding.gov.uk/council/councillors-and-committees/your-councillors/{alias}"

        # Create councillor
        councillor = self.add_councillor(
            url, identifier=alias, party=party, division=division, name=name
        )

        # Extract photo URL
        image_data = search_data.get("image", [{}])
        if image_data:
            file_path = image_data[0].get("file_path", "")
            if file_path:
                councillor.photo_url = f"https://www.eastriding.gov.uk{file_path}"

        # Fetch individual entry to get email (not included in bulk listing)
        entry_data = self.get(ENTRY_API_URL.format(alias=alias)).json()
        contact_data = entry_data.get("entry", {}).get("data", {}).get("contact", [{}])
        if contact_data:
            email = contact_data[0].get("emailWork", "") or contact_data[0].get(
                "email", ""
            )
            if email and isinstance(email, str):
                councillor.email = email

        return councillor
