import json
from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from lgsf.path_utils import scraper_abs_path


class SerializableDataclass(ABC):
    """Base class for dataclasses with common serialization methods."""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        """Create instance from dictionary, filtering to valid fields."""
        field_names = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        return {k: v for k, v in self.__dict__.items() if v is not None}

    def update_from_dict(self, data: Dict[str, Any]) -> bool:
        """Update from dictionary, return True if changes were made."""
        field_names = {f.name for f in self.__dataclass_fields__.values()}
        has_changes = False

        for key, value in data.items():
            if key in field_names and getattr(self, key) != value:
                setattr(self, key, value)
                has_changes = True

        return has_changes


@dataclass
class EveryElectionData(SerializableDataclass):
    """Data from EveryElection API."""

    url: Optional[str] = None
    official_identifier: Optional[str] = None
    organisation_type: Optional[str] = None
    organisation_subtype: Optional[str] = None
    official_name: Optional[str] = None
    common_name: Optional[str] = None
    slug: Optional[str] = None
    territory_code: Optional[str] = None
    election_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    created: Optional[str] = None


@dataclass
class ServiceData(SerializableDataclass):
    """Service metadata (CMS info, URLs, etc.)."""

    cms_type: Optional[str] = None
    cms_version: Optional[str] = None
    base_url: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class CouncilMetadata:
    """Council metadata with file I/O."""

    everyelection_data: EveryElectionData = field(default_factory=EveryElectionData)
    councillors: ServiceData = field(default_factory=ServiceData)
    everyelection_data_last_updated: Optional[str] = None

    @classmethod
    def from_file(cls, file_path: Path) -> "CouncilMetadata":
        """Load from JSON file."""
        if not file_path.exists():
            return cls()

        with open(file_path, "r") as f:
            data = json.load(f)

        metadata = cls()

        # Current format
        if "services" in data:
            if "everyelectiion_data" in data:
                metadata.everyelection_data = EveryElectionData.from_dict(
                    data["everyelectiion_data"]
                )

            councillors_data = data["services"].get("councillors", {})
            metadata.councillors = ServiceData.from_dict(councillors_data)

            metadata.everyelection_data_last_updated = data.get(
                "everyelectiion_data_last_updated"
            )

        # Legacy manual_data format
        elif "manual_data" in data:
            if "everyelectiion_data" in data:
                metadata.everyelection_data = EveryElectionData.from_dict(
                    data["everyelectiion_data"]
                )

            metadata.councillors = ServiceData.from_dict(data["manual_data"])
            metadata.everyelection_data_last_updated = data.get(
                "everyelection_data_last_updated"
            )

        # Very old format - all data at root
        else:
            metadata.everyelection_data = EveryElectionData.from_dict(data)

        return metadata

    def to_dict(self) -> Dict[str, Any]:
        """Convert to JSON-ready dictionary."""
        result = {
            "everyelectiion_data": self.everyelection_data.to_dict(),
            "services": {},
        }

        councillors_dict = self.councillors.to_dict()
        if councillors_dict:
            result["services"]["councillors"] = councillors_dict

        if self.everyelection_data_last_updated:
            result["everyelectiion_data_last_updated"] = (
                self.everyelection_data_last_updated
            )

        return result

    def save_to_file(self, file_path: Path) -> None:
        """Save to JSON file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(self.to_dict(), f, indent=4, sort_keys=True)

    def update_everyelection_data(self, new_data: Dict[str, Any]) -> None:
        """Update EveryElection data, setting timestamp if changes made."""
        new_data = new_data.copy()
        new_data.pop("modified", None)  # We track this separately

        if self.everyelection_data.update_from_dict(new_data):
            self.everyelection_data_last_updated = datetime.now().isoformat()

    def update_service_data(self, service_name: str, **kwargs) -> None:
        """Update service data fields."""
        if service_name == "councillors":
            self.councillors.update_from_dict(kwargs)

    def get_service_metadata(self, service_name: str) -> Optional[ServiceData]:
        """Get metadata for a specific service type."""
        if service_name == "councillors":
            return self.councillors
        return None

    @classmethod
    def for_council(cls, council_id: str) -> "CouncilMetadata":
        """Load or create metadata for a council."""
        try:
            scraper_path = scraper_abs_path(council_id)
            metadata_file = scraper_path / "metadata.json"
            return cls.from_file(metadata_file)
        except IOError:
            metadata = cls()
            metadata.everyelection_data.official_identifier = council_id
            return metadata

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of key metadata fields."""
        return {
            "official_identifier": self.everyelection_data.official_identifier,
            "common_name": self.everyelection_data.common_name,
            "cms_type": self.councillors.cms_type,
            "base_url": self.councillors.base_url,
            "services": ["councillors"] if self.councillors.to_dict() else [],
        }
