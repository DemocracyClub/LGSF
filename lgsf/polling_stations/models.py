import json


class PollingStationsList:
    def __init__(self, stations):
        self.stations = stations

    def as_file_name(self):
        return "stations"

    def as_json(self):
        return json.dumps(self.stations, indent=4)


class PollingDistrictsList:
    def __init__(self, districts):
        self.districts = districts

    def as_file_name(self):
        return "districts"

    def as_json(self):
        return json.dumps(self.districts, indent=4)
