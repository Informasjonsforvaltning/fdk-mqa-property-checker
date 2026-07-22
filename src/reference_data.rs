use cached::proc_macro::cached;
use http::{HeaderMap, HeaderValue};
use lazy_static::lazy_static;
use serde::de::DeserializeOwned;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;

const REFERENCE_DATA_CACHE_TTL_SECS: u64 = 86400;
const _: () = assert!(REFERENCE_DATA_CACHE_TTL_SECS == 86400);

lazy_static! {
    pub static ref REFERENCE_DATA_BASE_URL: String =
        env::var("REFERENCE_DATA_BASE_URL").unwrap_or("https://data.norge.no".to_string());
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
    #[allow(dead_code)]
    pub name: String,
    #[allow(dead_code)]
    pub r#type: String,
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub code: String,
    #[allow(dead_code)]
    #[serde(rename = "mediaType")]
    pub media_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenLicenseCollection {
    #[serde(rename = "openLicenses")]
    pub open_licenses: Vec<OpenLicense>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenLicense {
    pub uri: String,
    #[allow(dead_code)]
    pub code: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessRightsCollection {
    #[serde(rename = "accessRights")]
    pub access_rights: Vec<AccessRight>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessRight {
    pub uri: String,
    #[allow(dead_code)]
    pub code: String,
    #[allow(dead_code)]
    pub label: std::collections::HashMap<String, String>,
}

trait ReferenceDataItem {
    fn uri(&self) -> &str;
}

impl ReferenceDataItem for MediaType {
    fn uri(&self) -> &str {
        &self.uri
    }
}

impl ReferenceDataItem for FileType {
    fn uri(&self) -> &str {
        &self.uri
    }
}

impl ReferenceDataItem for OpenLicense {
    fn uri(&self) -> &str {
        &self.uri
    }
}

impl ReferenceDataItem for AccessRight {
    fn uri(&self) -> &str {
        &self.uri
    }
}

pub fn strip_http_scheme(uri: String) -> String {
    uri.replace("http://", "").replace("https://", "")
}

pub async fn valid_media_type(media_type: String) -> bool {
    match get_remote_media_types().await {
        Some(media_types) => media_types.contains_key(strip_http_scheme(media_type).as_str()),
        None => false,
    }
}

pub async fn valid_file_type(file_type: String) -> bool {
    match get_remote_file_types().await {
        Some(file_types) => file_types.contains_key(strip_http_scheme(file_type).as_str()),
        None => false,
    }
}

pub async fn valid_open_license(license: String) -> bool {
    match get_remote_open_licenses().await {
        Some(open_licenses) => open_licenses.contains_key(strip_http_scheme(license).as_str()),
        None => false,
    }
}

pub async fn valid_access_right(access_right: String) -> bool {
    match get_remote_access_rights().await {
        Some(access_rights) => access_rights.contains_key(strip_http_scheme(access_right).as_str()),
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

fn items_to_map<T: ReferenceDataItem>(items: Vec<T>) -> HashMap<String, T> {
    items
        .into_iter()
        .map(|item| (strip_http_scheme(item.uri().to_string()), item))
        .collect()
}

async fn fetch_reference_data<C, T, F>(
    path: &str,
    label: &str,
    extract: F,
) -> Option<HashMap<String, T>>
where
    C: DeserializeOwned,
    T: ReferenceDataItem,
    F: FnOnce(C) -> Vec<T>,
{
    let url = format!("{}{path}", REFERENCE_DATA_BASE_URL.as_str());
    let response = reqwest::Client::new()
        .get(url)
        .headers(construct_headers())
        .send()
        .await;

    match response {
        Ok(resp) => match resp.json::<C>().await {
            Ok(json) => Some(items_to_map(extract(json))),
            Err(e) => {
                tracing::warn!("Cannot get remote {label} {e}");
                None
            }
        },
        Err(e) => {
            tracing::warn!("Cannot get remote {label} {e}");
            None
        }
    }
}

#[cached(time = 86400)]
pub async fn get_remote_media_types() -> Option<HashMap<String, MediaType>> {
    fetch_reference_data(
        "/reference-data/iana/media-types",
        "media-types",
        |json: MediaTypeCollection| json.media_types,
    )
    .await
}

#[cached(time = 86400)]
pub async fn get_remote_file_types() -> Option<HashMap<String, FileType>> {
    fetch_reference_data(
        "/reference-data/eu/file-types",
        "file-types",
        |json: FileTypeCollection| json.file_types,
    )
    .await
}

#[cached(time = 86400)]
pub async fn get_remote_open_licenses() -> Option<HashMap<String, OpenLicense>> {
    fetch_reference_data(
        "/reference-data/open-licenses",
        "open-licenses",
        |json: OpenLicenseCollection| json.open_licenses,
    )
    .await
}

#[cached(time = 86400)]
pub async fn get_remote_access_rights() -> Option<HashMap<String, AccessRight>> {
    fetch_reference_data(
        "/reference-data/eu/access-rights",
        "access-rights",
        |json: AccessRightsCollection| json.access_rights,
    )
    .await
}
