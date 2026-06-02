import re

from lgsf.councillors.scrapers import HTMLCouncillorScraper

SKIP_HEADINGS = {"Your Cabinet"}


def decode_cfemail(hexstr: str) -> str:
    data = bytes.fromhex(hexstr)
    key = data[0]
    return bytes(b ^ key for b in data[1:]).decode("utf-8")


class Scraper(HTMLCouncillorScraper):
    verify_requests = False
    list_page = {
        "container_css_selector": "main",
        "councillor_css_selector": "h4",
    }

    def get_councillors(self):
        page = self.get_page(self.base_url)
        results = []

        for h3 in page.select("main h3"):
            ward_name = h3.get_text(strip=True)
            # Skip cabinet heading and MP headings (contain em-dash)
            if ward_name in SKIP_HEADINGS or "–" in ward_name:
                continue

            accordion = h3.find_next_sibling()
            if not accordion:
                continue

            buttons = accordion.select(".kt-blocks-accordion-header")
            panels = accordion.select(".kt-accordion-panel-inner")

            for button, panel in zip(buttons, panels):
                if button.get_text(strip=True) != "Councillors":
                    continue

                for h4 in panel.select("h4"):
                    heading = h4.get_text(strip=True)
                    party_match = re.search(r"\(([^)]+)\)\s*$", heading)
                    if not party_match:
                        continue

                    party = party_match.group(1)
                    name_raw = heading[: party_match.start()].strip()
                    name_raw = re.sub(r"^Cllr\s+", "", name_raw).strip()
                    if "," in name_raw:
                        last, first = name_raw.split(",", 1)
                        name = f"{first.strip()} {last.strip()}"
                    else:
                        name = name_raw

                    identifier = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
                    url = f"{self.base_url}#{identifier}"

                    data = {
                        "url": url,
                        "identifier": identifier,
                        "name": name,
                        "party": party,
                        "ward": ward_name,
                    }

                    next_div = h4.find_next_sibling()
                    if next_div:
                        cf = next_div.select_one("a[href*='email-protection']")
                        if cf and "#" in cf["href"]:
                            hexstr = cf["href"].split("#", 1)[-1]
                            data["email"] = decode_cfemail(hexstr)

                    prev_fig = h4.find_previous_sibling("figure")
                    if prev_fig:
                        img = prev_fig.select_one("img")
                        src = img.get("src", "") if img else ""
                        if src and not src.startswith("data:"):
                            data["photo_url"] = src

                    results.append(data)

        return results

    def get_single_councillor(self, councillor_data):
        councillor = self.add_councillor(
            councillor_data["url"],
            identifier=councillor_data["identifier"],
            name=councillor_data["name"],
            party=councillor_data["party"],
            division=councillor_data["ward"],
        )
        if "email" in councillor_data:
            councillor.email = councillor_data["email"]
        if "photo_url" in councillor_data:
            councillor.photo_url = councillor_data["photo_url"]
        return councillor
