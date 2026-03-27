use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser, RangeQuery};
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

// Datenstrukturen fuer Flutter
#[derive(Debug, Clone)]
pub struct Document {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub score: f32,
    pub doc: Document,
}

struct TantivyApi {
    index: Index,
    writer: Mutex<Option<IndexWriter>>,
    reader: IndexReader,
    schema: Schema,
    id_field: Field,
    text_field: Field,
}

static STATE: Lazy<Arc<Mutex<Option<TantivyApi>>>> = Lazy::new(|| Arc::new(Mutex::new(None)));

#[flutter_rust_bridge::frb(sync)]
pub fn init_tantivy(dir_path: String) -> Result<()> {
    let mut state_lock = STATE.lock().unwrap();
    if state_lock.is_some() {
        return Ok(());
    }

    let index_dir = PathBuf::from(dir_path);
    std::fs::create_dir_all(&index_dir)?;

    let existing = index_dir.join("meta.json").exists();

    let (index, schema) = if existing {
        let index = Index::open_in_dir(&index_dir)?;
        let schema = index.schema();
        (index, schema)
    } else {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("id", STRING | STORED);
        schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_dir(&index_dir, schema.clone())?;
        (index, schema)
    };

    let id_field = schema
        .get_field("id")
        .map_err(|_| anyhow!("'id' field not found"))?;
    let text_field = schema
        .get_field("text")
        .map_err(|_| anyhow!("'text' field not found"))?;

    // Writer nur erstellen wenn der Index neu angelegt wurde.
    // Beim reinen Lesen (lupa.exe) kein Writer → kein Lock → mehrere Nutzer gleichzeitig.
    let writer = if existing {
        None
    } else {
        Some(index.writer(50_000_000)?)
    };

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;

    let api = TantivyApi {
        index,
        writer: Mutex::new(writer),
        reader,
        schema,
        id_field,
        text_field,
    };

    *state_lock = Some(api);

    Ok(())
}

/// Erkennt Einzelwort-Wildcards (z.B. "heterozygot*") im Query-String
/// und ersetzt sie durch Range-Queries, da Tantivys QueryParser fuer
/// Einzelwort-Prefixe einen PhrasePrefixRequiresAtLeastTwoTerms-Fehler wirft.
///
/// Bei Multi-Wort-Phrasen mit Prefix ("big bad wo"*) bleibt PhrasePrefixQuery
/// via den normalen QueryParser erhalten.
fn build_prefix_range_query(field: Field, prefix_text: &str) -> Box<dyn tantivy::query::Query> {
    // Tantivy's default tokenizer: lowercase
    let lower = prefix_text.to_lowercase();
    let lower_term = Term::from_field_text(field, &lower);

    // Obere Grenze: letztes Byte inkrementieren
    // Wir bauen den upper-bound Term ueber from_field_text mit dem inkrementierten String.
    let upper_bound = {
        let bytes = lower.as_bytes();
        let mut end = bytes.to_vec();
        while let Some(last) = end.last_mut() {
            if *last < 0xFF {
                *last += 1;
                break;
            } else {
                end.pop();
            }
        }
        if end.is_empty() {
            Bound::Unbounded
        } else {
            // end ist jetzt ein UTF-8-aehnliches Byte-Array. Fuer rein ASCII-Prefixe
            // (was bei medizinischen Termen der Fall ist) ist das valides UTF-8.
            // Fuer nicht-ASCII nutzen wir den naechsten gueltigen String.
            match String::from_utf8(end) {
                Ok(end_str) => Bound::Excluded(Term::from_field_text(field, &end_str)),
                Err(_) => {
                    // Fallback: unbounded (findet evtl. etwas zu viel, aber korrekt)
                    Bound::Unbounded
                }
            }
        }
    };

    Box::new(RangeQuery::new(Bound::Included(lower_term), upper_bound))
}

/// Zerlegt den Query-String, findet Einzelwort-Wildcards und baut einen
/// kombinierten Query: Range-Queries fuer Wildcards, QueryParser fuer den Rest.
fn build_query_with_wildcard_fix(
    index: &Index,
    text_field: Field,
    raw_query: &str,
) -> Result<Box<dyn tantivy::query::Query>> {
    // Tokenize the query manually to find single-word wildcards like "pathogen*"
    // vs multi-word phrases or other syntax
    //
    // Strategie: Wir parsen den Query mit parse_query_lenient und ersetzen
    // fehlgeschlagene Wildcard-Terme durch Range-Queries.
    //
    // Einfacher Ansatz: Wir splitten den Query an Whitespace und AND/OR/NOT,
    // erkennen Einzelwort-Wildcards, und bauen den finalen Query zusammen.

    // Pruefe ob ueberhaupt ein Wildcard im Query vorkommt
    if !raw_query.contains('*') {
        // Kein Wildcard – normaler QueryParser reicht
        let query_parser = QueryParser::for_index(index, vec![text_field]);
        let query = query_parser.parse_query(raw_query)?;
        return Ok(query);
    }

    // Strategie: parse_query_lenient nutzen. Das gibt uns einen Query + Fehlerliste.
    // Wenn Fehler vom Typ PhrasePrefixRequiresAtLeastTwoTerms auftreten, wissen wir
    // dass Einzelwort-Wildcards gescheitert sind.
    //
    // Problem: lenient mode verwirft die fehlgeschlagenen Leaves komplett.
    // Wir muessen die Wildcards daher VOR dem Parsen extrahieren und separat behandeln.

    // Schritt 1: Extrahiere alle Einzelwort-Wildcard-Terme
    // Ein Einzelwort-Wildcard ist: ein Token das mit * endet, kein Leerzeichen enthaelt,
    // und nicht in Anfuehrungszeichen steht.
    let mut wildcard_prefixes: Vec<String> = Vec::new();
    let mut remaining_parts: Vec<String> = Vec::new();
    let mut in_quotes = false;

    for ch in raw_query.chars() {
        if ch == '"' {
            in_quotes = !in_quotes;
        }
    }
    // Reset – wir brauchen einen tokenbasierten Ansatz
    in_quotes = false;

    // Einfacher State-Machine-Parser
    let mut current_token = String::new();
    let mut result_tokens: Vec<String> = Vec::new();

    for ch in raw_query.chars() {
        if ch == '"' {
            in_quotes = !in_quotes;
            current_token.push(ch);
        } else if ch.is_whitespace() && !in_quotes {
            if !current_token.is_empty() {
                result_tokens.push(current_token.clone());
                current_token.clear();
            }
        } else {
            current_token.push(ch);
        }
    }
    if !current_token.is_empty() {
        result_tokens.push(current_token);
    }

    // Schritt 2: Identifiziere Einzelwort-Wildcards vs Rest
    for token in &result_tokens {
        let trimmed = token.trim();
        // Ist es ein Einzelwort-Wildcard? (kein Operator, keine Klammer-only, endet mit *)
        if trimmed.ends_with('*')
            && !trimmed.starts_with('"')
            && trimmed != "*"
            && !trimmed.eq_ignore_ascii_case("AND")
            && !trimmed.eq_ignore_ascii_case("OR")
            && !trimmed.eq_ignore_ascii_case("NOT")
        {
            // Strip fuehrende +/- und Klammern
            let cleaned = trimmed
                .trim_start_matches('+')
                .trim_start_matches('-')
                .trim_start_matches('(')
                .trim_end_matches(')');
            // Entferne field-prefix wie "text:"
            let prefix_text = if let Some(pos) = cleaned.find(':') {
                &cleaned[pos + 1..]
            } else {
                cleaned
            };
            // Entferne das abschliessende *
            let prefix = prefix_text.trim_end_matches('*');
            if !prefix.is_empty() {
                wildcard_prefixes.push(prefix.to_string());
                // Ersetze den Wildcard im remaining Query durch einen Platzhalter
                // der garantiert matcht (wir verknuepfen spaeter per AND)
                remaining_parts.push("*".to_string()); // Tantivy's "match all"
            } else {
                remaining_parts.push(trimmed.to_string());
            }
        } else {
            remaining_parts.push(trimmed.to_string());
        }
    }

    if wildcard_prefixes.is_empty() {
        // Keine echten Einzelwort-Wildcards gefunden – normaler Parser
        let query_parser = QueryParser::for_index(index, vec![text_field]);
        let query = query_parser.parse_query(raw_query)?;
        return Ok(query);
    }

    // Schritt 3: Parse den Rest-Query (ohne Wildcards, mit * als Match-All)
    let remaining_query = remaining_parts.join(" ");

    // Baue den Rest-Query. Ersetze die "*" Platzhalter nicht – sie matchen alles,
    // was OK ist, weil wir die Wildcards separat per AND verknuepfen.
    // Allerdings wollen wir die Semantik (AND/OR/NOT) erhalten.
    //
    // Besserer Ansatz: Entferne die *-Platzhalter und baue den restlichen Query
    // nur aus den nicht-Wildcard-Termen.
    let non_wildcard_parts: Vec<&str> = result_tokens
        .iter()
        .filter(|t| {
            let trimmed = t.trim();
            // Behalte alles ausser Einzelwort-Wildcards
            !(trimmed.ends_with('*')
                && !trimmed.starts_with('"')
                && trimmed != "*"
                && !trimmed.eq_ignore_ascii_case("AND")
                && !trimmed.eq_ignore_ascii_case("OR")
                && !trimmed.eq_ignore_ascii_case("NOT"))
        })
        .map(|s| s.as_str())
        .collect();

    // Entferne alleinstehende Operatoren am Anfang/Ende
    let clean_non_wildcard = clean_dangling_operators(&non_wildcard_parts);

    let mut must_clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

    // Range-Queries fuer jede Wildcard
    for prefix in &wildcard_prefixes {
        must_clauses.push((Occur::Must, build_prefix_range_query(text_field, prefix)));
    }

    // Rest-Query (wenn vorhanden)
    if !clean_non_wildcard.is_empty() {
        let rest_str = clean_non_wildcard.join(" ");
        if !rest_str.trim().is_empty()
            && rest_str.trim() != "AND"
            && rest_str.trim() != "OR"
            && rest_str.trim() != "NOT"
        {
            let query_parser = QueryParser::for_index(index, vec![text_field]);
            // Lenient: ignoriert kaputte Teile statt abzubrechen
            let (rest_query, _errors) = query_parser.parse_query_lenient(&rest_str);
            must_clauses.push((Occur::Must, rest_query));
        }
    }

    if must_clauses.is_empty() {
        // Fallback: nur Wildcards, kein Rest
        if must_clauses.is_empty() && !wildcard_prefixes.is_empty() {
            // Nur eine Wildcard → direkt zurueckgeben
            return Ok(build_prefix_range_query(text_field, &wildcard_prefixes[0]));
        }
        let query_parser = QueryParser::for_index(index, vec![text_field]);
        let query = query_parser.parse_query(raw_query)?;
        return Ok(query);
    }

    if must_clauses.len() == 1 {
        return Ok(must_clauses.into_iter().next().unwrap().1);
    }

    Ok(Box::new(BooleanQuery::new(must_clauses)))
}

/// Entfernt alleinstehende Operatoren am Anfang/Ende einer Token-Liste
fn clean_dangling_operators<'a>(tokens: &[&'a str]) -> Vec<&'a str> {
    let mut result: Vec<&str> = tokens.to_vec();

    // Entferne fuehrende Operatoren
    while !result.is_empty() {
        let first = result[0].trim().to_uppercase();
        if first == "AND" || first == "OR" || first == "NOT" {
            result.remove(0);
        } else {
            break;
        }
    }

    // Entferne abschliessende Operatoren
    while !result.is_empty() {
        let last = result.last().unwrap().trim().to_uppercase();
        if last == "AND" || last == "OR" || last == "NOT" {
            result.pop();
        } else {
            break;
        }
    }

    // Entferne doppelte Operatoren ("AND AND" → "AND")
    let mut cleaned: Vec<&str> = Vec::new();
    let mut last_was_op = false;
    for t in &result {
        let upper = t.trim().to_uppercase();
        let is_op = upper == "AND" || upper == "OR" || upper == "NOT";
        if is_op && last_was_op {
            continue; // doppelten Operator ueberspringen
        }
        cleaned.push(t);
        last_was_op = is_op;
    }

    cleaned
}

// ---- Oeffentliche API-Funktionen ----

pub fn search_documents(query: String, top_k: usize) -> Result<Vec<SearchResult>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    api.reader.reload()?;
    let searcher = api.reader.searcher();

    // Benutze den Wildcard-Fix statt direkt QueryParser::parse_query
    let query = build_query_with_wildcard_fix(&api.index, api.text_field, &query)?;

    let top_docs = searcher.search(&query, &TopDocs::with_limit(top_k))?;

    let mut results = Vec::new();
    for (score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc::<TantivyDocument>(doc_address)?;
        let id = retrieved_doc
            .get_first(api.id_field)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let text = retrieved_doc
            .get_first(api.text_field)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        results.push(SearchResult {
            score,
            doc: Document { id, text },
        });
    }

    Ok(results)
}

pub fn add_document(doc: Document) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;

    let id_term = Term::from_field_text(api.id_field, &doc.id);
    writer.delete_term(id_term.clone());

    let mut tantivy_doc = TantivyDocument::new();
    tantivy_doc.add_text(api.id_field, &doc.id);
    tantivy_doc.add_text(api.text_field, &doc.text);

    writer.add_document(tantivy_doc)?;
    writer.commit()?;

    Ok(())
}

#[flutter_rust_bridge::frb(sync)]
pub fn get_document_by_id(id: String) -> Result<Option<Document>> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let searcher = api.reader.searcher();

    let id_term = Term::from_field_text(api.id_field, &id);
    let query = tantivy::query::TermQuery::new(id_term, IndexRecordOption::Basic);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(1))?;

    if let Some((_, doc_address)) = top_docs.first() {
        let retrieved_doc = searcher.doc::<TantivyDocument>(*doc_address)?;
        let text = retrieved_doc
            .get_first(api.text_field)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        return Ok(Some(Document { id, text }));
    }

    Ok(None)
}

pub fn update_document(doc: Document) -> Result<()> {
    add_document(doc)
}

pub fn delete_document(id: String) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;
    let id_term = Term::from_field_text(api.id_field, &id);

    writer.delete_term(id_term);
    writer.commit()?;

    Ok(())
}

pub fn add_documents_batch(docs: Vec<Document>) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;

    for doc in docs {
        let id_term = Term::from_field_text(api.id_field, &doc.id);
        writer.delete_term(id_term);

        let mut tantivy_doc = TantivyDocument::new();
        tantivy_doc.add_text(api.id_field, &doc.id);
        tantivy_doc.add_text(api.text_field, &doc.text);

        writer.add_document(tantivy_doc)?;
    }

    writer.commit()?;

    Ok(())
}

pub fn delete_documents_batch(ids: Vec<String>) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;

    for id in ids {
        let id_term = Term::from_field_text(api.id_field, &id);
        writer.delete_term(id_term);
    }

    writer.commit()?;

    Ok(())
}

#[flutter_rust_bridge::frb(sync)]
pub fn commit() -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;
    writer.commit()?;

    Ok(())
}

pub fn add_document_no_commit(doc: Document) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;

    let id_term = Term::from_field_text(api.id_field, &doc.id);
    writer.delete_term(id_term);

    let mut tantivy_doc = TantivyDocument::new();
    tantivy_doc.add_text(api.id_field, &doc.id);
    tantivy_doc.add_text(api.text_field, &doc.text);

    writer.add_document(tantivy_doc)?;

    Ok(())
}

pub fn delete_document_no_commit(id: String) -> Result<()> {
    let state_lock = STATE.lock().unwrap();
    let api = state_lock
        .as_ref()
        .ok_or_else(|| anyhow!("Tantivy not initialized"))?;

    let mut writer_guard = api.writer.lock().unwrap();
    let writer = writer_guard
        .as_mut()
        .ok_or_else(|| anyhow!("Index im Lesemodus geoeffnet – Schreiben nicht moeglich"))?;
    let id_term = Term::from_field_text(api.id_field, &id);

    writer.delete_term(id_term);

    Ok(())
}
