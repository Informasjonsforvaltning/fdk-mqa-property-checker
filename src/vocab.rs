pub mod dcterms {
    use oxigraph::model::NamedNodeRef;

    pub const ACCESS_RIGHTS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/accessRights");

    pub const FORMAT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/format");

    pub const SUBJECT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/subject");

    pub const PUBLISHER: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/publisher");

    pub const SPATIAL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/spatial");

    pub const TEMPORAL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/temporal");

    pub const ISSUED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/issued");

    pub const MODIFIED: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/modified");

    pub const RIGHTS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/rights");

    pub const LICENSE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://purl.org/dc/terms/license");
}

pub mod dcat {
    use oxigraph::model::NamedNodeRef;

    pub const DATASET_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#Dataset");

    pub const DISTRIBUTION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#distribution");

    pub const THEME: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#theme");

    pub const CONTACT_POINT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#contactPoint");

    pub const KEYWORD: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#keyword");

    pub const BYTE_SIZE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#byteSize");

    pub const DOWNLOAD_URL: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#downloadURL");

    pub const MEDIA_TYPE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dcat#mediaType");
}

pub mod dqv {
    use oxigraph::model::NamedNodeRef;

    pub const QUALITY_MEASUREMENT_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#QualityMeasurement");

    pub const QUALITY_ANNOTATION_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#QualityAnnotation");

    pub const IS_MEASUREMENT_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#isMeasurementOf");

    pub const COMPUTED_ON: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#computedOn");

    pub const VALUE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/dqv#value");
}

pub mod dcat_mqa {
    use oxigraph::model::NamedNodeRef;

    // Assessment
    pub const DATASET_ASSESSMENT_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment");

    pub const DISTRIBUTION_ASSESSMENT_CLASS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment");

    pub const HAS_DISTRIBUTION_ASSESSMENT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment");   

    pub const CONTAINS_QUALITY_MEASUREMENT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement");

    pub const CONTAINS_QUALITY_ANNOTATION: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#containsQualityAnnotation");   

    pub const ASSESSMENT_OF: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf");   

    // Stars    
    pub const ZERO_STARS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#zeroStars");

    pub const ONE_STAR: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#oneStar");

    pub const TWO_STARS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#twoStars");

    pub const THREE_STARS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#threeStars");

    pub const FOUR_STARS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#fourStars");

    pub const FIVE_STARS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#fiveStars");

    // Findability
    pub const KEYWORD_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability",
    );

    pub const CATEGORY_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#categoryAvailability",
    );

    pub const SPATIAL_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#spatialAvailability",
    );

    pub const TEMPORAL_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#temporalAvailability",
    );

    // Accessibility
    pub const DOWNLOAD_URL_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability",
    );

    // Interoperability
    pub const FORMAT_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability",
    );

    pub const MEDIA_TYPE_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#mediaTypeAvailability",
    );

    pub const FORMAT_MEDIA_TYPE_VOCABULARY_ALIGNMENT: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked(
            "https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeVocabularyAlignment",
        );

    pub const FORMAT_MEDIA_TYPE_NON_PROPRIETARY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeNonProprietary",
    );

    pub const FORMAT_MEDIA_TYPE_MACHINE_INTERPRETABLE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked(
            "https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeMachineInterpretable",
        );

    pub const AT_LEAST_FOUR_STARS: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars");

    // Reusability
    pub const LICENSE_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#licenseAvailability",
    );

    pub const KNOWN_LICENSE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#knownLicense");

    pub const OPEN_LICENSE: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("https://data.norge.no/vocabulary/dcatno-mqa#openLicense");

    pub const ACCESS_RIGHTS_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#accessRightsAvailability",
    );

    pub const ACCESS_RIGHTS_VOCABULARY_ALIGNMENT: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#accessRightsVocabularyAlignment",
    );

    pub const CONTACT_POINT_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#contactPointAvailability",
    );

    pub const PUBLISHER_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#publisherAvailability",
    );

    // Contextuality
    pub const RIGHTS_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#rightsAvailability",
    );

    pub const BYTE_SIZE_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#byteSizeAvailability",
    );

    pub const DATE_ISSUED_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability",
    );

    pub const DATE_MODIFIED_AVAILABILITY: NamedNodeRef<'_> = NamedNodeRef::new_unchecked(
        "https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability",
    );
}

pub mod prov {
    use oxigraph::model::NamedNodeRef;

    pub const WAS_DERIVED_FROM: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/prov#wasDerivedFrom");
}

pub mod oa {
    use oxigraph::model::NamedNodeRef;

    pub const HAS_BODY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/oa#hasBody");

    pub const MOTIVATED_BY: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/oa#motivatedBy");

    pub const CLASSIFYING: NamedNodeRef<'_> =
        NamedNodeRef::new_unchecked("http://www.w3.org/ns/oa#classifying");
}
