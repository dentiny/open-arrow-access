use chrono::NaiveDate;
use roxmltree::{Document, Node};

use open_arrow_access_core::Result;

use super::decode;

const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const PG: &str = "http://www.gutenberg.org/2009/pgterms/";
const DC: &str = "http://purl.org/dc/terms/";
const DCAM: &str = "http://purl.org/dc/dcam/";

pub(super) struct Creator {
    pub uri: Option<String>,
    pub name: Option<String>,
    pub alias: Vec<String>,
    pub birthdate: Option<i32>,
    pub deathdate: Option<i32>,
}

pub(super) struct Subject {
    pub scheme: Option<String>,
    pub value: String,
}

pub(super) struct File {
    pub uri: String,
    pub format: Vec<String>,
    pub extent: Option<u64>,
    pub modified: Option<String>,
}

pub(super) struct Book {
    pub ebook_id: u64,
    pub title: Option<String>,
    pub creator: Vec<Creator>,
    pub language: Vec<String>,
    pub subject: Vec<Subject>,
    pub bookshelf: Vec<String>,
    pub issued: Option<i32>,
    pub rights: Option<String>,
    pub files: Vec<File>,
}

fn children<'a, 'input>(
    node: Node<'a, 'input>,
    namespace: &'a str,
    name: &'a str,
) -> impl Iterator<Item = Node<'a, 'input>> {
    node.children()
        .filter(move |child| child.has_tag_name((namespace, name)))
}

fn text(node: Node<'_, '_>, namespace: &str, name: &str) -> Option<String> {
    children(node, namespace, name)
        .next()
        .and_then(|child| child.text())
        .map(str::to_owned)
}

fn values(node: Node<'_, '_>, namespace: &str, name: &str) -> Vec<String> {
    children(node, namespace, name)
        .filter_map(|child| child.text().map(str::to_owned))
        .collect()
}

fn described_values(node: Node<'_, '_>, namespace: &str, name: &str) -> Vec<String> {
    children(node, namespace, name)
        .filter_map(|child| {
            children(child, RDF, "Description")
                .next()
                .and_then(|description| text(description, RDF, "value"))
        })
        .collect()
}

fn number<T: std::str::FromStr>(value: Option<String>, field: &str) -> Result<Option<T>> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|_| decode(format!("invalid {field}: {value:?}")))
        })
        .transpose()
}

/// Parse book RDF using namespace URIs.
pub(super) fn parse(raw: &[u8]) -> Result<Book> {
    let xml = std::str::from_utf8(raw)
        .map_err(|error| decode("metadata is not UTF-8").with_source(error))?;
    // DTDs are rejected; external entities are never fetched.
    let document =
        Document::parse(xml).map_err(|error| decode("invalid RDF XML").with_source(error))?;
    let root = document.root_element();
    if !root.has_tag_name((RDF, "RDF")) {
        return Err(decode("expected an RDF document"));
    }
    let mut books = children(root, PG, "ebook");
    let book = books
        .next()
        .ok_or_else(|| decode("missing Gutenberg ebook"))?;
    if books.next().is_some() {
        return Err(decode("expected exactly one ebook per RDF object"));
    }
    let about = book.attribute((RDF, "about")).unwrap_or_default();
    let id = about
        .strip_prefix("ebooks/")
        .or_else(|| about.strip_prefix("http://www.gutenberg.org/ebooks/"))
        .or_else(|| about.strip_prefix("https://www.gutenberg.org/ebooks/"))
        .ok_or_else(|| decode("invalid ebook URI"))?;
    if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(decode("invalid ebook ID"));
    }
    let ebook_id = id
        .parse::<u64>()
        .ok()
        .filter(|id| *id != 0)
        .ok_or_else(|| decode("invalid ebook ID"))?;
    let creator = children(book, DC, "creator")
        .map(|creator| {
            let agent = children(creator, PG, "agent")
                .next()
                .ok_or_else(|| decode("missing creator agent"))?;
            Ok(Creator {
                uri: agent.attribute((RDF, "about")).map(str::to_owned),
                name: text(agent, PG, "name"),
                alias: values(agent, PG, "alias"),
                birthdate: number(text(agent, PG, "birthdate"), "birthdate")?,
                deathdate: number(text(agent, PG, "deathdate"), "deathdate")?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let subject = children(book, DC, "subject")
        .map(|subject| {
            let description = children(subject, RDF, "Description")
                .next()
                .ok_or_else(|| decode("missing subject description"))?;
            Ok(Subject {
                scheme: children(description, DCAM, "memberOf")
                    .next()
                    .and_then(|node| node.attribute((RDF, "resource")))
                    .map(str::to_owned),
                value: text(description, RDF, "value")
                    .ok_or_else(|| decode("missing subject value"))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let issued = text(book, DC, "issued")
        .map(|value| {
            NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                .map(|date| {
                    date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                        .num_days() as i32
                })
                .map_err(|error| decode("invalid issued date").with_source(error))
        })
        .transpose()?;
    let mut files = Vec::new();
    for format in children(book, DC, "hasFormat") {
        let file = children(format, PG, "file")
            .next()
            .ok_or_else(|| decode("missing file description"))?;
        let uri = file
            .attribute((RDF, "about"))
            .filter(|uri| !uri.is_empty())
            .ok_or_else(|| decode("missing file URI"))?
            .to_owned();
        if files.iter().any(|file: &File| file.uri == uri) {
            return Err(decode("duplicate file URI in RDF"));
        }
        files.push(File {
            uri,
            format: described_values(file, DC, "format"),
            extent: number(text(file, DC, "extent"), "extent")?,
            modified: text(file, DC, "modified"),
        });
    }
    Ok(Book {
        ebook_id,
        title: text(book, DC, "title"),
        creator,
        language: described_values(book, DC, "language"),
        subject,
        bookshelf: described_values(book, PG, "bookshelf"),
        issued,
        rights: text(book, DC, "rights"),
        files,
    })
}
