"""
The document pipeline: extract a stored document's text with Docling,
split it into chunks, embed the chunks, and write the results beside the
document. See docs/document-pipeline.md.

The unit of work is a file: (council, storage_key). Nothing here knows
about meetings or decisions.

Only ``extract`` and ``documents`` need the ``embeddings`` dependency
group (Docling, torch); they import it inside functions, so the rest of
the package imports without it.
"""
