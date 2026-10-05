# Database fixtures

Fixture scripts belong to the component tests that require them. No executable
fixtures are present in the scaffold.

The PostgreSQL phase needs:

- Dedicated yasp_primary and yasp_secondary databases for target isolation.
- Least-privileged yasp_restricted and yasp_analyst login roles.
- Tables covering NULLs, numeric precision, timestamps/time zones, text, booleans,
  UUIDs, JSON, binary data, and arrays exercised by client compatibility scenarios.
- Grants, row-level security, and restricted views demonstrating different source
  access for the selected login roles.
- Metadata/policy cases selecting each role through one BI connection, plus denied
  mappings and a restricted route for schema discovery.
- Different target data to expose accidental cross-target cache reuse.

Each later engine supplies equivalent scenarios through its own fixture setup.
Tests use isolated data and credentials, with documented teardown.
