# Changelog

## [0.1.1] - 2026-03-27

### Fixed
- **Writer-Lock-Bug:** `init_tantivy()` erstellte immer einen `IndexWriter`, auch wenn nur gelesen wird. Das sperrte den Index – nur eine App-Instanz konnte gleichzeitig laufen. Fix: `IndexWriter` ist jetzt `Option<IndexWriter>`, wird nur erstellt wenn der Index neu angelegt wird. Bestehende Indizes werden read-only geöffnet → beliebig viele Nutzer gleichzeitig.
- **Wildcard-Bug:** Einzelwort-Wildcards wie `heterozygot*` warfen `PhrasePrefixRequiresAtLeastTwoTerms`. Fix: Einzelwort-Wildcards werden als `RangeQuery` (Prefix-Match) behandelt statt als `PhrasePrefixQuery`.

## [0.1.0] - 2025-11-03

### Added
- Initial release (Upstream: JAICHANGPARK/flutter_tantivy)
- Full-text search powered by Tantivy (Rust)
- CRUD operations: `addDocument()`, `getDocumentById()`, `updateDocument()`, `deleteDocument()`
- Batch operations: `addDocumentsBatch()`, `deleteDocumentsBatch()`
- Transaction control: `addDocumentNoCommit()`, `deleteDocumentNoCommit()`, `commit()`
- `searchDocuments()` with boolean operators (AND, OR, NOT), phrase search, wildcards
- `initTantivy()` – persistent index, thread-safe, auto-reload
- Cross-platform: Android, iOS, macOS, Linux, Windows
- Built with flutter_rust_bridge 2.11.1