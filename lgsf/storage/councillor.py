import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from slugify import slugify

from .base import CouncillorStorage, StorageConfig

@dataclass
class CouncillorData:
    """Data class for councillor information"""
    url: str
    identifier: str
    name: str
    party: str
    division: str
    email: Optional[str] = None
    photo_url: Optional[str] = None
    standing_down: Optional[bool] = None

    def as_file_name(self) -> str:
        """Generate a file name for the councillor"""
        return f"{slugify(self.identifier)}-{slugify(self.name)}"

    def as_dict(self) -> Dict:
        """Convert councillor data to dictionary"""
        out = {
            "photo_url": self.photo_url,
            "email": self.email,
            "standing_down": self.standing_down,
            "url": self.url,
        }
        RAW_FIELDS = ["identifier", "name", "party", "division"]
        for attr in RAW_FIELDS:
            out[f"raw_{attr}"] = getattr(self, attr)
        return out

    def as_json(self) -> str:
        """Convert councillor data to JSON string"""
        return json.dumps(self.as_dict(), indent=4)

    def as_csv(self) -> str:
        """Convert councillor data to CSV string"""
        import io
        out = io.StringIO()
        out_csv = csv.writer(out)
        out_csv.writerow(["identifier", "name", "party", "division"])
        out_csv.writerow([self.identifier, self.name, self.party, self.division])
        return out.getvalue()

    @classmethod
    def from_dict(cls, data: Dict) -> 'CouncillorData':
        """Create CouncillorData from dictionary"""
        # Copy to avoid mutating input
        data = dict(data)
        email = data.pop("email", None)
        photo_url = data.pop("photo_url", None)
        standing_down = data.pop("standing_down", None)

        # Handle raw_ prefixed fields
        for k in list(data.keys()):
            if k.startswith("raw_"):
                data[k[4:]] = data.pop(k)

        instance = cls(
            url=data["url"],
            identifier=data["identifier"],
            name=data["name"],
            party=data["party"],
            division=data["division"],
            email=email,
            photo_url=photo_url,
            standing_down=standing_down
        )
        return instance

class CouncillorStorage(CouncillorStorage):
    """Storage backend for councillor data"""

    def __init__(self, config: StorageConfig):
        super().__init__(config)
        self.councillors: List[CouncillorData] = []

    def save(self, path: str, content: Any) -> None:
        """Save councillor data"""
        if isinstance(content, CouncillorData):
            self.councillors.append(content)
        else:
            raise ValueError("Content must be an instance of CouncillorData")

    def load(self, path: str) -> Optional[CouncillorData]:
        """Load councillor data from file"""
        if not path.endswith('.json'):
            return None

        try:
            with open(path, 'r') as f:
                data = json.load(f)
            return CouncillorData.from_dict(data)
        except (json.JSONDecodeError, FileNotFoundError):
            return None

    def delete(self, path: str) -> None:
        """Delete councillor data"""
        # Implementation depends on the underlying storage
        pass

    def exists(self, path: str) -> bool:
        """Check if councillor data exists"""
        # Implementation depends on the underlying storage
        return False

    def get_all_councillors(self) -> List[CouncillorData]:
        """Get all stored councillors"""
        return self.councillors

    def export_to_csv(self, output_path: str) -> None:
        """Export all councillors to CSV"""
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['identifier', 'name', 'party', 'division'])
            for councillor in self.councillors:
                writer.writerow([
                    councillor.identifier,
                    councillor.name,
                    councillor.party,
                    councillor.division
                ])
