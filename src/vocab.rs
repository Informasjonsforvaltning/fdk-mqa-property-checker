#[macro_export]
macro_rules! n {
    ($iri:expr) => {
        oxigraph::model::NamedNodeRef::new_unchecked($iri)
    };
}

type N = oxigraph::model::NamedNodeRef<'static>;

pub mod dcterms {
    use super::N;

    pub const ACCESS_RIGHTS: N = n!("http://purl.org/dc/terms/accessRights");
    pub const FORMAT: N = n!("http://purl.org/dc/terms/format");
    pub const SUBJECT: N = n!("http://purl.org/dc/terms/subject");
    pub const PUBLISHER: N = n!("http://purl.org/dc/terms/publisher");
    pub const SPATIAL: N = n!("http://purl.org/dc/terms/spatial");
    pub const TEMPORAL: N = n!("http://purl.org/dc/terms/temporal");
    pub const ISSUED: N = n!("http://purl.org/dc/terms/issued");
    pub const MODIFIED: N = n!("http://purl.org/dc/terms/modified");
    pub const RIGHTS: N = n!("http://purl.org/dc/terms/rights");
    pub const LICENSE: N = n!("http://purl.org/dc/terms/license");
}

pub mod dcat {
    use super::N;

    pub const DATASET_CLASS: N = n!("http://www.w3.org/ns/dcat#Dataset");
    pub const DISTRIBUTION: N = n!("http://www.w3.org/ns/dcat#distribution");
    pub const THEME: N = n!("http://www.w3.org/ns/dcat#theme");
    pub const CONTACT_POINT: N = n!("http://www.w3.org/ns/dcat#contactPoint");
    pub const KEYWORD: N = n!("http://www.w3.org/ns/dcat#keyword");
    pub const BYTE_SIZE: N = n!("http://www.w3.org/ns/dcat#byteSize");
    pub const DOWNLOAD_URL: N = n!("http://www.w3.org/ns/dcat#downloadURL");
    pub const MEDIA_TYPE: N = n!("http://www.w3.org/ns/dcat#mediaType");
}

pub mod dqv {
    use super::N;

    pub const QUALITY_MEASUREMENT_CLASS: N = n!("http://www.w3.org/ns/dqv#QualityMeasurement");
    pub const QUALITY_ANNOTATION_CLASS: N = n!("http://www.w3.org/ns/dqv#QualityAnnotation");
    pub const IS_MEASUREMENT_OF: N = n!("http://www.w3.org/ns/dqv#isMeasurementOf");
    pub const COMPUTED_ON: N = n!("http://www.w3.org/ns/dqv#computedOn");
    pub const VALUE: N = n!("http://www.w3.org/ns/dqv#value");
}

pub mod dcat_mqa {
    use super::N;

    // Assessment
    pub const ASSESSMENT_OF: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf");
    pub const HAS_ASSESSMENT: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#hasAssessment");
    pub const DATASET_ASSESSMENT_CLASS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment");
    pub const DISTRIBUTION_ASSESSMENT_CLASS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment");
    pub const HAS_DISTRIBUTION_ASSESSMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment");
    pub const CONTAINS_QUALITY_MEASUREMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement");
    pub const CONTAINS_QUALITY_ANNOTATION: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#containsQualityAnnotation");

    // Stars
    pub const ZERO_STARS: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#zeroStars");
    pub const ONE_STAR: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#oneStar");
    pub const TWO_STARS: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#twoStars");
    pub const THREE_STARS: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#threeStars");
    pub const FOUR_STARS: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#fourStars");
    pub const FIVE_STARS: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#fiveStars");

    // Findability
    pub const KEYWORD_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability");
    pub const CATEGORY_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#categoryAvailability");
    pub const SPATIAL_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#spatialAvailability");
    pub const TEMPORAL_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#temporalAvailability");

    // Accessibility
    pub const DOWNLOAD_URL_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability");

    // Interoperability
    pub const FORMAT_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability");
    pub const MEDIA_TYPE_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#mediaTypeAvailability");
    pub const FORMAT_MEDIA_TYPE_VOCABULARY_ALIGNMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeVocabularyAlignment");
    pub const FORMAT_MEDIA_TYPE_NON_PROPRIETARY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeNonProprietary");
    pub const FORMAT_MEDIA_TYPE_MACHINE_INTERPRETABLE: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeMachineInterpretable");
    pub const AT_LEAST_FOUR_STARS: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars");

    // Reusability
    pub const LICENSE_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#licenseAvailability");
    pub const KNOWN_LICENSE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#knownLicense");
    pub const OPEN_LICENSE: N = n!("https://data.norge.no/vocabulary/dcatno-mqa#openLicense");
    pub const ACCESS_RIGHTS_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#accessRightsAvailability");
    pub const ACCESS_RIGHTS_VOCABULARY_ALIGNMENT: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#accessRightsVocabularyAlignment");
    pub const CONTACT_POINT_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#contactPointAvailability");
    pub const PUBLISHER_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#publisherAvailability");

    // Contextuality
    pub const RIGHTS_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#rightsAvailability");
    pub const BYTE_SIZE_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#byteSizeAvailability");
    pub const DATE_ISSUED_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability");
    pub const DATE_MODIFIED_AVAILABILITY: N =
        n!("https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability");
}

pub mod prov {
    use super::N;

    pub const WAS_DERIVED_FROM: N = n!("http://www.w3.org/ns/prov#wasDerivedFrom");
}

pub mod oa {
    use super::N;

    pub const HAS_BODY: N = n!("http://www.w3.org/ns/oa#hasBody");
    pub const MOTIVATED_BY: N = n!("http://www.w3.org/ns/oa#motivatedBy");
    pub const CLASSIFYING: N = n!("http://www.w3.org/ns/oa#classifying");
}
