# Councillor scrapers

The scraper classes for councillor data, and the objects they
produce. For running them, see [running-scrapers.md](running-scrapers.md).

There are 4 types of councillor scraper class supported.

All scrapers require a `base_url` property on the class. You can optionally
set a list of `tags` and a `disabled` flag on the class.

### `BaseCouncillorScraper`

This is the most basic scraper class. It requires two methods:
`get_councillors` and `get_single_councillor`.

`get_councillors` must return a iterator that contains the raw content
representing a councillor.

Each item returned will be passed to `get_single_councillor`.

`get_single_councillor` must return a `Councillor` object.

If this pattern doesn’t work for a council, then the required `run` method can
be overridden. Run needs to call `self.save_councillor` with the raw scraped
data for each councillor, and a councillor object. It then needs to call
`self.report()`.

### `HTMLCouncillorScraper`

Expects a dict containing CSS selectors for example:

```python
list_page = {
	"container_css_selector": ".container .col-md-8",
	"councillor_css_selector": ".col-sm-3",
}
```

Where `container_css_selector` is the CSS selector for the elements that
contains a list of councillors, and `councillor_css_selector` is the selector
for each element that contains a single councillor.

The `get_single_councillor` method is required, and needs to return
`Councillor` object.

### PagedHTMLCouncillorScraper

A subclass of `HTMLCouncillorScraper` that supports pagination in on the
container page (the list of coucillors is split over different pages).

Looks for a new key in `self.list_page` called `"next_page_css_selector"`
and uses that to iterate over the pages calling `get_single_councillor` for
each as it goes.

### CMISCouncillorScraper

This is a scraper sub-class for councils using CMIS. You can tell these
because they normally have `CMIS` in the URL somewhere.

All that should be required is a `base_url`. This normally ends in
`Councillors.aspx` or `Members.aspx` and is the list of councillors per ward.

### ModGovCouncillorScraper

Similar to the CMIS class, this scrapes ModernGov URLs. ModernGov sites have
URLs that contain something like `mgMemberIndex.aspx`. If `mg` is in the URL,
it’s likely it’s a ModGov site. You can test this by looking for the WDSL page.

If the URL with councillors on is
`http://democracy.kirklees.gov.uk/mgMemberIndex.aspx` then try requesting
`http://democracy.kirklees.gov.uk/ mgWebService.asmx?WSDL`. If you see XML,
then it's using ModGov with the API turned on.

The `base_url` should be the URL above the `mgWebService.asmx` script, e.g.
`http://democracy.kirklees.gov.uk/` or
`http://democracy.kirklees.gov.uk/councillors/` if it’s installed at a
sub-path.


## Skipping councillors

The contract of `get_single_councillor` is that it must return a
`Councillor` object.

However in some cases the source requires that this can't happen. Two examples:

1. With `HTMLCouncillorScraper` when iteration over all rows in a table, we
   sometimes see inline header rows. They can get passed to
   `get_single_councillor`, but don't contain a councillor
2. Adur and Worthing have one page for all their councillors and we need to
   remove Adur councillors from Worthing's scraper and vise versa.

To deal with this we have `SkipCouncillorException`. If
`get_single_councillor` raises this exception then the loop continues on to
the next councillor.

## Councillor objects

All scrapers in some way need to make a set of councillor objects.
`CMISCouncillorScraper` and `ModGovCouncillorScraper` handle this
automatically, but the `HTMLCouncillorScraper` and `BaseCouncillorScraper`
don’t.

Councillor objects require a `url`, `identifier`, `name`, `party` and
`division`:

```python
from lgsf.councillors import CouncillorBase

councillor = CouncillorBase(
    url,
    identifier=identifier,
    name=name,
    party=party,
    division=division,
)
```

All councillor scrapers need to have `self.councillors = set()`, and each
scraper needs to populate this with `Councillor` objects.
