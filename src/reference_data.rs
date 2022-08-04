use cached::proc_macro::cached;
use http::{HeaderMap, HeaderValue};
use lazy_static::lazy_static;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;

lazy_static! {
    pub static ref REFERENCE_DATA_BASE_URL: String = env::var("REFERENCE_DATA_BASE_URL")
        .unwrap_or("https://data.norge.no/new-reference-data".to_string());
    pub static ref REFERENCE_DATA_API_KEY: String =
        env::var("REFERENCE_DATA_API_KEY").unwrap_or("".to_string());
}

#[derive(Debug, Clone, Deserialize)]
pub struct MediaTypeCollection {
    #[serde(rename = "mediaTypes")]
    pub media_types: Vec<MediaType>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MediaType {
    pub uri: String,
    pub name: String,
    pub r#type: String,
    #[serde(rename = "subType")]
    pub sub_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileTypeCollection {
    #[serde(rename = "fileTypes")]
    pub file_types: Vec<FileType>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FileType {
    pub uri: String,
    pub code: String,
    #[serde(rename = "mediaType")]
    pub media_type: String,
}

pub fn strip_http_scheme(uri: String) -> String {
    uri.replace("http://", "").replace("https://", "")
}

pub fn valid_media_type(media_type: String) -> bool {
    match get_remote_media_types() {
        Some(media_types) => media_types.contains_key(strip_http_scheme(media_type).as_str()),
        None => false,
    }
}

pub fn valid_file_type(file_type: String) -> bool {
    match get_remote_file_types() {
        Some(file_types) => file_types.contains_key(strip_http_scheme(file_type).as_str()),
        None => false,
    }
}

fn construct_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-API-KEY",
        HeaderValue::from_static(&REFERENCE_DATA_API_KEY),
    );
    headers
}

#[cached(time = 86400)]
pub fn get_remote_media_types() -> Option<HashMap<String, MediaType>> {
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/iana/media-types", REFERENCE_DATA_BASE_URL.to_string()).as_str())
        .headers(construct_headers())
        .send();

    match response {
        Ok(resp) => match resp.json::<MediaTypeCollection>() {
            Ok(json) => Some(
                json.media_types
                    .into_iter()
                    .map(|ft| (strip_http_scheme(ft.uri.clone()), ft))
                    .collect::<HashMap<String, MediaType>>(),
            ),
            Err(e) => {
                tracing::warn!("Cannot get remote media-types {}", e);
                None
            }
        },
        Err(e) => {
            tracing::warn!("Cannot get remote media-types {}", e);
            None
        }
    }
}

#[cached(time = 86400)]
pub fn get_remote_file_types() -> Option<HashMap<String, FileType>> {
    let response = reqwest::blocking::Client::new()
        .get(format!("{}/eu/file-types", REFERENCE_DATA_BASE_URL.to_string()).as_str())
        .headers(construct_headers())
        .send();

    match response {
        Ok(resp) => match resp.json::<FileTypeCollection>() {
            Ok(json) => Some(
                json.file_types
                    .into_iter()
                    .map(|ft| (strip_http_scheme(ft.uri.clone()), ft))
                    .collect::<HashMap<String, FileType>>(),
            ),
            Err(e) => {
                tracing::warn!("Cannot get remote file-types {}", e);
                None
            }
        },
        Err(e) => {
            tracing::warn!("Cannot get remote file-types {}", e);
            None
        }
    }
}
