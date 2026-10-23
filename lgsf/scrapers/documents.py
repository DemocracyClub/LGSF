"""
Downloading the documents a scraped record links to, for any data type.
"""

from lgsf.storage.documents import is_content_key


class DocumentSavingMixin:
    """
    Downloads the documents linked from a record to the document store, and
    records on each document where it went.

    For a scraper class, before ScraperBase:

        class BaseThingScraper(DocumentSavingMixin, ScraperBase):

    It needs ``self.index["documents"]``, a dict that the scraper saves
    between runs, as minutes and decisions do in ``_index.json``. It maps
    each document's URL to what was recorded for it, and is how a later run
    knows not to download a document again.

    Documents are stored under a key made from their bytes (see
    BaseDocumentStorage.store), so the same file is stored once per council
    whichever record, or data type, linked it. A scraper never builds a
    key.
    """

    documents_linked = 0
    documents_downloaded = 0
    documents_skipped = 0
    documents_failed = 0
    documents_unavailable = 0
    documents_bytes = 0
    documents_already_in_store = 0

    def stored_document_metadata(self, url):
        """
        Return the recorded metadata for an already-stored document, or None.

        The index is only trusted as far as the document store agrees with
        it: an entry whose file has gone is treated as never fetched.
        """
        entry = self.index["documents"].get(url)
        if not entry:
            return None

        key = entry.get("storage_key")
        if not key or not is_content_key(key):
            # A key from before documents were named after their content.
            # The S3 migration renames those; until it has, the document
            # is fetched again and stored under its content key.
            return None

        try:
            if not self.document_storage.exists(key):
                return None
        except ValueError:
            # Key written by a backend with a different key format
            return None

        return entry

    def save_record_documents(self, record):
        """
        Store every document linked from ``record``, skipping any we
        already hold, and record where each one went on the document.

        A document's key comes from its bytes, so one the index doesn't
        name is downloaded to find out whether it is held. If it is - the
        same file linked from somewhere else, or one stored by a run that
        stopped before its index was saved - it isn't written again.

        A failed download shouldn't fail the whole scrape. Returns how many
        documents could not be fetched this time but might be next time.
        A 404 or 410 is not one of those: it is recorded on the document as
        ``unavailable``. Nor is a 200 with no body, recorded as
        ``unavailable: "empty"``, since fetching it again doesn't help.
        """
        missing = 0
        for document in getattr(record, "documents", []):
            url = document.get("url")
            if not url:
                continue

            existing = self.stored_document_metadata(url)
            if existing:
                # Already downloaded by an earlier run and still in the
                # store: reuse its metadata so a skipped document looks
                # exactly like a freshly fetched one.
                document.update(existing)
                self.documents_skipped += 1
                continue

            # Not a conditional request: we only get here when the index
            # doesn't name a stored file, so a 304 would only cost a second
            # request.
            try:
                response = self.get(
                    url, extra_headers=self.extra_headers, raise_for_status=False
                )
                status = self.response_status(response)
                if status in (404, 410):
                    document["unavailable"] = status
                    self.documents_unavailable += 1
                    continue
                response.raise_for_status()
                content = self.response_bytes(response)
            except Exception as e:
                missing += 1
                self.documents_failed += 1
                self.console.log(f"[yellow]Failed to download {url}: {e}[/yellow]")
                continue

            if not content:
                # Some councils answer 200 with no body for a document
                # (4,202 on S3), and the ones fetched again later were
                # still empty, so there is nothing to store or retry.
                document["unavailable"] = "empty"
                self.documents_unavailable += 1
                continue

            stored, written = self.document_storage.store(content)

            entry = stored.as_dict()
            entry["etag"] = self.response_header(response, "etag")
            entry["last_modified"] = self.response_header(response, "last-modified")
            entry["content_type"] = self.response_header(response, "content-type")

            document.update(entry)
            self.index["documents"][url] = entry
            self.documents_downloaded += 1
            self.documents_bytes += len(content)
            if not written:
                self.documents_already_in_store += 1
        return missing
