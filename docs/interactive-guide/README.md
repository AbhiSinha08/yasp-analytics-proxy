# Interactive runtime guide

Serve this folder from the repository root:

```sh
python3 -m http.server 8000 --bind 127.0.0.1 --directory docs/interactive-guide
```

Open <http://localhost:8000/>. No installation or build is required.

`index.html` embeds all five Archify readers on one page. Theme, motion,
zoom, inspection and export stay inside each diagram. Startup and refresh,
compute routing, and data context share a three-tab panel.

Click a query node to open its example beside it, or use the step selector.
The note shows SQL and data, with an Inputs & output tab explaining what the
step extracts, uses and produces. Close the note to see the complete graph.
The translation example is attached to its arrow. Extra database glyphs inside
the single Source execution node indicate multiple configured sources.

Customization notes spell out **Configurable Callback**. SQL translation and
provider compute provisioning/suspension are planned. Source connection pooling
and ordered overflow within a selected compute class are implemented. The
illustrative `light_query` and `heavy_query` classes map to small and large
instances; each permitted login can use either configured class.

The active diagrams are `runtime-components`, `query-dependencies`,
`startup-refresh`, `compute-routing`, and `data-context-sources`. Their JSON
files contain source references, and their generated HTML files are the
embedded readers. The wrapper adds example notes, readable labels and source
glyphs at runtime; it does not alter the generated readers. Older SVG exports
in `assets/` are not used by the current page.

Archify receipts under `evidence/` cover the native readers. The embedded-page
browser review is recorded separately and bound to the current wrapper bytes.
SQL, policy mappings, users, logins, instance sizes and model scores are examples.
