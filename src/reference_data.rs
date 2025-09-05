use cached::proc_macro::cached;
use http::{HeaderMap, HeaderValue};
use lazy_static::lazy_static;
use serde_derive::Deserialize;
use std::collections::HashMap;
use std::env;

lazy_static! {
    pub static ref REFERENCE_DATA_BASE_URL: String = env::var("REFERENCE_DATA_BASE_URL")
        .unwrap_or("https://data.norge.no".to_string());
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

#[cached(time = 86400)]
pub async fn get_remote_media_types() -> Option<HashMap<String, MediaType>> {
    let response = reqwest::Client::new()
        .get(format!("{}/reference-data/iana/media-types", REFERENCE_DATA_BASE_URL.to_string()).as_str())
        .headers(construct_headers())
        .send()
        .await;

    match response {
        Ok(resp) => match resp.json::<MediaTypeCollection>().await {
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
pub async fn get_remote_file_types() -> Option<HashMap<String, FileType>> {
    let response = reqwest::Client::new()
        .get(format!("{}/reference-data/eu/file-types", REFERENCE_DATA_BASE_URL.to_string()).as_str())
        .headers(construct_headers())
        .send()
        .await;

    match response {
        Ok(resp) => match resp.json::<FileTypeCollection>().await {
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

#[cached(time = 86400)]
pub async fn get_remote_open_licenses() -> Option<HashMap<String, OpenLicense>> {
    let response = reqwest::Client::new()
        .get(format!("{}/reference-data/open-licenses", REFERENCE_DATA_BASE_URL.to_string()).as_str())
        .headers(construct_headers())
        .send()
        .await;

    match response {
        Ok(resp) => match resp.json::<OpenLicenseCollection>().await {
            Ok(json) => Some(
                json.open_licenses
                    .into_iter()
                    .map(|ft| (strip_http_scheme(ft.uri.clone()), ft))
                    .collect::<HashMap<String, OpenLicense>>(),
            ),
            Err(e) => {
                tracing::warn!("Cannot get remote open-licenses {}", e);
                None
            }
        },
        Err(e) => {
            tracing::warn!("Cannot get remote open-licenses {}", e);
            None
        }
    }
}

#[cached(time = 86400)]
pub async fn get_remote_access_rights() -> Option<HashMap<String, AccessRight>> {
    let response = reqwest::Client::new()
        .get(format!("{}/reference-data/eu/access-rights", REFERENCE_DATA_BASE_URL.to_string()).as_str())
        .headers(construct_headers())
        .send()
        .await;

    match response {
        Ok(resp) => match resp.json::<AccessRightsCollection>().await {
            Ok(json) => Some(
                json.access_rights
                    .into_iter()
                    .map(|ar| (strip_http_scheme(ar.uri.clone()), ar))
                    .collect::<HashMap<String, AccessRight>>(),
            ),
            Err(e) => {
                tracing::warn!("Cannot get remote access-rights {}", e);
                None
            }
        },
        Err(e) => {
            tracing::warn!("Cannot get remote access-rights {}", e);
            None
        }
    }
}
