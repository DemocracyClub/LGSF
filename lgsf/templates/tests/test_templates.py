"""
Scaffolding templates.

Every template is driven end to end through the command, so a break in
the command itself surfaces here rather than the first time somebody
scaffolds a scraper.
"""

import ast
import io

import pytest

from lgsf.conf import settings
from lgsf.templates.commands import Command
from lgsf.templates.helpers import TEMPLATES


@pytest.fixture
def council_dir(tmp_path, monkeypatch):
    scrapers_dir = tmp_path / "scrapers"
    path = scrapers_dir / "ZZQ-anytown"
    path.mkdir(parents=True)
    monkeypatch.setattr(settings, "SCRAPER_DIR_NAME", str(scrapers_dir))
    return path


@pytest.mark.parametrize("template_name", sorted(TEMPLATES))
def test_every_template_scaffolds_valid_python(council_dir, template_name):
    Command(
        [
            "templates",
            "--council",
            "ZZQ",
            "--template",
            template_name,
            "--context",
            "base_url",
            "https://example.gov.uk",
        ],
        io.StringIO(),
    ).execute()

    written = council_dir / TEMPLATES[template_name].file_name
    assert written.exists(), f"{template_name} wrote nothing"
    ast.parse(written.read_text())


@pytest.mark.parametrize("template_name", sorted(TEMPLATES))
def test_every_template_uses_the_base_url_it_is_given(council_dir, template_name):
    Command(
        [
            "templates",
            "--council",
            "ZZQ",
            "--template",
            template_name,
            "--context",
            "base_url",
            "https://example.gov.uk",
        ],
        io.StringIO(),
    ).execute()

    written = council_dir / TEMPLATES[template_name].file_name
    assert 'base_url = "https://example.gov.uk"' in written.read_text()


@pytest.mark.parametrize("template_name", sorted(TEMPLATES))
def test_every_template_defines_a_scraper_class(council_dir, template_name):
    Command(
        [
            "templates",
            "--council",
            "ZZQ",
            "--template",
            template_name,
            "--context",
            "base_url",
            "https://example.gov.uk",
        ],
        io.StringIO(),
    ).execute()

    tree = ast.parse((council_dir / TEMPLATES[template_name].file_name).read_text())
    classes = [n.name for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]
    assert "Scraper" in classes


def test_minutes_and_councillor_templates_both_exist():
    """A hack day needs both; only councillors had templates before."""
    assert {name for name in TEMPLATES if name.startswith("minutes_")} == {
        "minutes_scraper_modgov",
        "minutes_scraper_cmis",
        "minutes_scraper_custom",
    }


def test_an_unknown_template_lists_the_real_ones(council_dir):
    command = Command(
        ["templates", "--council", "ZZQ", "--template", "nope"], io.StringIO()
    )
    with pytest.raises(ValueError, match="not a valid template name"):
        command.execute()


def test_an_existing_scraper_is_not_overwritten(council_dir):
    (council_dir / "minutes.py").write_text("# hand written, do not clobber\n")

    command = Command(
        [
            "templates",
            "--council",
            "ZZQ",
            "--template",
            "minutes_scraper_modgov",
            "--context",
            "base_url",
            "https://example.gov.uk",
        ],
        io.StringIO(),
    )
    with pytest.raises(ValueError, match="not overwriting"):
        command.execute()

    assert "hand written" in (council_dir / "minutes.py").read_text()
