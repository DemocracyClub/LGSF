"""
Simple runtime utilities for scrapers to access their metadata.
"""

from pathlib import Path
from typing import Optional

from .models import CouncilMetadata, ServiceData


def get_council_metadata(council_id: str) -> Optional[CouncilMetadata]:
    """Get metadata for a council, return None if not found."""
    try:
        return CouncilMetadata.for_council(council_id)
    except Exception:
        return None


def get_service_metadata(council_id: str, service_name: str) -> Optional[ServiceData]:
    """Get service metadata for a council and service type."""
    metadata = get_council_metadata(council_id)
    if metadata:
        return metadata.get_service_metadata(service_name)
    return None


def get_councillors_metadata(council_id: str) -> Optional[ServiceData]:
    """Get councillors service metadata for a council."""
    return get_service_metadata(council_id, "councillors")


def check_cms_type(council_id: str, service_name: str, expected_cms: str) -> bool:
    """
    Check if stored CMS type matches expected type.
    Returns True if match or no stored type (allowing any CMS).
    """
    service_metadata = get_service_metadata(council_id, service_name)
    if not service_metadata or not service_metadata.cms_type:
        return True  # No stored CMS type, allow any
    return service_metadata.cms_type.lower() == expected_cms.lower()


def get_base_url(council_id: str, service_name: str) -> Optional[str]:
    """Get base URL for a service, or None if not set."""
    service_metadata = get_service_metadata(council_id, service_name)
    return service_metadata.base_url if service_metadata else None
