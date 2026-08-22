from unittest.mock import MagicMock

from lgsf.interests.models import RegisterOfInterestsBase


def test_model_instantiation():
    record = RegisterOfInterestsBase(
        url="https://example.gov.uk/rofi?UID=123",
        identifier="123",
        councillor_name="Cllr Jane Doe",
        councillor_id="123",
        councillor_url="https://example.gov.uk/user?UID=123",
        division="Central",
    )
    record.interests = [
        {
            "category": "Employment",
            "headers": ["Me", "Partner"],
            "rows": [["Teacher", "None"]],
        }
    ]
    record.published_date = "Monday, 1 January 2026"
    record.documents = []

    assert record.identifier == "123"
    assert record.councillor_name == "Cllr Jane Doe"
    assert record.as_file_name() == "123-cllr-jane-doe"
    assert "RegisterOfInterests: Cllr Jane Doe (123)" in repr(record)


def test_model_serialization_and_deserialization():
    record = RegisterOfInterestsBase(
        url="https://example.gov.uk/rofi?UID=123",
        identifier="123",
        councillor_name="Cllr Jane Doe",
        councillor_id="123",
        councillor_url="https://example.gov.uk/user?UID=123",
        division="Central",
    )
    record.interests = [
        {
            "category": "Employment",
            "headers": ["Me"],
            "rows": [["Engineer"]],
        }
    ]
    record.published_date = "Monday, 1 January 2026"
    record.documents = [{"title": "Form", "url": "https://example.gov.uk/form.pdf"}]

    as_dict = record.as_dict()
    assert as_dict["url"] == "https://example.gov.uk/rofi?UID=123"
    assert as_dict["raw_councillor_name"] == "Cllr Jane Doe"
    assert as_dict["raw_councillor_id"] == "123"
    assert len(as_dict["interests"]) == 1
    assert len(as_dict["documents"]) == 1

    as_json = record.as_json()
    assert isinstance(as_json, str)

    session = MagicMock()
    session.open.return_value = as_json
    loaded = RegisterOfInterestsBase.from_storage("dummy.json", session)

    assert loaded.identifier == "123"
    assert loaded.councillor_name == "Cllr Jane Doe"
    assert loaded.division == "Central"
    assert loaded.interests == record.interests
    assert loaded.documents == record.documents
    assert loaded.published_date == record.published_date
