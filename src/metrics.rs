use crate::{
    error::Error,
    rdf::{
        add_derived_from, add_five_star_annotation, add_property, add_quality_measurement,
        dump_graph_as_turtle, formats_include_rdf, get_dataset_node, get_five_star_annotation,
        has_property, insert_dataset_assessment, insert_distribution_assessment,
        list_access_right_uris, list_distributions, list_format_uris, list_license_uris,
        list_media_type_uris, node_assessment, parse_turtle,
    },
    reference_data::{valid_access_right, valid_file_type, valid_media_type, valid_open_license},
    vocab::{dcat, dcat_mqa, dcterms, oa},
};
use futures::StreamExt;
use oxigraph::{
    model::{BlankNode, NamedNodeRef, Quad, Term},
    store::Store,
};

struct AvailabilityCheck {
    metric: NamedNodeRef<'static>,
    properties: &'static [NamedNodeRef<'static>],
}

const DATASET_AVAILABILITY_CHECKS: &[AvailabilityCheck] = &[
    AvailabilityCheck {
        metric: dcat_mqa::ACCESS_RIGHTS_AVAILABILITY,
        properties: &[dcterms::ACCESS_RIGHTS],
    },
    AvailabilityCheck {
        metric: dcat_mqa::CATEGORY_AVAILABILITY,
        properties: &[dcat::THEME],
    },
    AvailabilityCheck {
        metric: dcat_mqa::CONTACT_POINT_AVAILABILITY,
        properties: &[dcat::CONTACT_POINT],
    },
    AvailabilityCheck {
        metric: dcat_mqa::KEYWORD_AVAILABILITY,
        properties: &[dcat::KEYWORD, dcterms::SUBJECT],
    },
    AvailabilityCheck {
        metric: dcat_mqa::PUBLISHER_AVAILABILITY,
        properties: &[dcterms::PUBLISHER],
    },
    AvailabilityCheck {
        metric: dcat_mqa::SPATIAL_AVAILABILITY,
        properties: &[dcterms::SPATIAL],
    },
    AvailabilityCheck {
        metric: dcat_mqa::TEMPORAL_AVAILABILITY,
        properties: &[dcterms::TEMPORAL],
    },
    AvailabilityCheck {
        metric: dcat_mqa::DATE_ISSUED_AVAILABILITY,
        properties: &[dcterms::ISSUED],
    },
    AvailabilityCheck {
        metric: dcat_mqa::DATE_MODIFIED_AVAILABILITY,
        properties: &[dcterms::MODIFIED],
    },
];

const DISTRIBUTION_AVAILABILITY_CHECKS: &[AvailabilityCheck] = &[
    AvailabilityCheck {
        metric: dcat_mqa::BYTE_SIZE_AVAILABILITY,
        properties: &[dcat::BYTE_SIZE],
    },
    AvailabilityCheck {
        metric: dcat_mqa::DATE_ISSUED_AVAILABILITY,
        properties: &[dcterms::ISSUED],
    },
    AvailabilityCheck {
        metric: dcat_mqa::DATE_MODIFIED_AVAILABILITY,
        properties: &[dcterms::MODIFIED],
    },
    AvailabilityCheck {
        metric: dcat_mqa::DOWNLOAD_URL_AVAILABILITY,
        properties: &[dcat::DOWNLOAD_URL],
    },
    AvailabilityCheck {
        metric: dcat_mqa::RIGHTS_AVAILABILITY,
        properties: &[dcterms::RIGHTS],
    },
    AvailabilityCheck {
        metric: dcat_mqa::FORMAT_AVAILABILITY,
        properties: &[dcterms::FORMAT],
    },
    AvailabilityCheck {
        metric: dcat_mqa::LICENSE_AVAILABILITY,
        properties: &[dcterms::LICENSE],
    },
    AvailabilityCheck {
        metric: dcat_mqa::MEDIA_TYPE_AVAILABILITY,
        properties: &[dcat::MEDIA_TYPE],
    },
];

async fn any_valid_in_reference_data<F, Fut>(items: Vec<String>, validator: F) -> bool
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    futures::stream::iter(items)
        .any(|item| async { validator(item).await })
        .await
}

async fn valid_format_or_media_type(value: String) -> bool {
    valid_file_type(value.clone()).await || valid_media_type(value).await
}

fn add_availability_measurements(
    checks: &[AvailabilityCheck],
    assessment_node: NamedNodeRef<'_>,
    subject_node: NamedNodeRef<'_>,
    store: &Store,
    output_store: &Store,
) -> Result<(), Error> {
    for check in checks {
        add_quality_measurement(
            check.metric,
            assessment_node,
            subject_node.into(),
            check
                .properties
                .iter()
                .any(|property| has_property(subject_node.into(), *property, store)),
            output_store,
        )?;
    }

    Ok(())
}

struct FiveStarInputs {
    is_open_license: bool,
    is_format_machine_interpretable: bool,
    is_format_non_proprietary: bool,
    is_format_rdf: bool,
    /// Linked-data resource check not yet implemented.
    has_linked_resources: bool,
}

struct FormatCheckResult {
    is_format_machine_interpretable: bool,
    is_format_non_proprietary: bool,
    is_format_rdf: bool,
    machine_interpretable_derived_from: Option<BlankNode>,
    non_proprietary_derived_from: Option<BlankNode>,
}

struct LicenseCheckResult {
    is_open_license: bool,
    open_license_derived_from: Option<BlankNode>,
}

struct FiveStarDerivedFrom {
    open_license: Option<BlankNode>,
    machine_interpretable: Option<BlankNode>,
    non_proprietary: Option<BlankNode>,
}

fn determine_star_rating(inputs: &FiveStarInputs) -> NamedNodeRef<'static> {
    if !inputs.is_open_license {
        return dcat_mqa::ZERO_STARS;
    }
    if !inputs.is_format_machine_interpretable {
        return dcat_mqa::ONE_STAR;
    }
    if !inputs.is_format_non_proprietary {
        return dcat_mqa::TWO_STARS;
    }
    if !inputs.is_format_rdf {
        return dcat_mqa::THREE_STARS;
    }
    if inputs.has_linked_resources {
        dcat_mqa::FIVE_STARS
    } else {
        dcat_mqa::FOUR_STARS
    }
}

fn attach_five_star_rating(
    dist_assessment_node: NamedNodeRef<'_>,
    dist_node: NamedNodeRef<'_>,
    metrics_store: &Store,
    inputs: &FiveStarInputs,
    derived_from: &FiveStarDerivedFrom,
) -> Result<(), Error> {
    let five_star_quality_annotation = add_five_star_annotation(metrics_store)?;
    let rating = determine_star_rating(inputs);

    if let Some(derived) = &derived_from.open_license {
        add_derived_from(
            five_star_quality_annotation.as_ref().into(),
            derived.as_ref().into(),
            metrics_store,
        )?;
    }

    if inputs.is_open_license {
        if let Some(derived) = &derived_from.machine_interpretable {
            add_derived_from(
                five_star_quality_annotation.as_ref().into(),
                derived.as_ref().into(),
                metrics_store,
            )?;
        }

        if inputs.is_format_machine_interpretable {
            if let Some(derived) = &derived_from.non_proprietary {
                add_derived_from(
                    five_star_quality_annotation.as_ref().into(),
                    derived.as_ref().into(),
                    metrics_store,
                )?;
            }
        }
    }

    add_quality_measurement(
        dcat_mqa::AT_LEAST_FOUR_STARS,
        dist_assessment_node,
        dist_node.into(),
        rating == dcat_mqa::FIVE_STARS || rating == dcat_mqa::FOUR_STARS,
        metrics_store,
    )?;

    add_property(
        five_star_quality_annotation.as_ref().into(),
        oa::HAS_BODY,
        rating.into(),
        metrics_store,
    )?;

    add_property(
        five_star_quality_annotation.as_ref().into(),
        oa::MOTIVATED_BY,
        oa::CLASSIFYING.into(),
        metrics_store,
    )?;

    Ok(())
}

pub async fn parse_rdf_graph_and_calculate_metrics(
    input_store: &Store,
    output_store: &Store,
    graph: String,
) -> Result<String, Error> {
    parse_turtle(input_store, graph)?;
    let dataset_node = get_dataset_node(input_store).ok_or("Dataset node not found in graph")?;
    calculate_metrics(dataset_node.as_ref(), input_store, output_store).await?;
    let bytes = dump_graph_as_turtle(output_store)?;
    let turtle = std::str::from_utf8(bytes.as_slice())
        .map_err(|e| format!("Failed converting graph to string: {}", e))?;
    Ok(turtle.to_string())
}

async fn calculate_metrics(
    dataset_node: NamedNodeRef<'_>,
    input_store: &Store,
    output_store: &Store,
) -> Result<(), Error> {
    let dataset_assessment = node_assessment(input_store, dataset_node)?;

    insert_dataset_assessment(dataset_assessment.as_ref(), dataset_node, &output_store)?;

    add_availability_measurements(
        DATASET_AVAILABILITY_CHECKS,
        dataset_assessment.as_ref(),
        dataset_node,
        input_store,
        output_store,
    )?;

    let access_rights = list_access_right_uris(dataset_node, input_store);
    let has_access_rights_property =
        has_property(dataset_node.into(), dcterms::ACCESS_RIGHTS, input_store);
    let is_access_rights_aligned = if has_access_rights_property {
        any_valid_in_reference_data(access_rights, valid_access_right).await
    } else {
        false
    };

    add_quality_measurement(
        dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT,
        dataset_assessment.as_ref(),
        dataset_node.into(),
        is_access_rights_aligned,
        &output_store,
    )?;

    for dist_quad in
        list_distributions(dataset_node, input_store).collect::<Result<Vec<Quad>, _>>()?
    {
        let distribution = if let Term::NamedNode(node) = dist_quad.object.clone() {
            node
        } else {
            tracing::warn!("distribution is not a named node");
            continue;
        };

        let distribution_assessment = node_assessment(input_store, distribution.as_ref())?;
        insert_distribution_assessment(
            dataset_assessment.as_ref(),
            distribution_assessment.as_ref(),
            distribution.as_ref(),
            &output_store,
        )?;

        calculate_distribution_metrics(
            distribution_assessment.as_ref(),
            distribution.as_ref(),
            input_store,
            output_store,
        )
        .await?;
    }

    match get_five_star_annotation(output_store) {
        Some(five_star_annotation) => {
            add_property(
                dataset_assessment.as_ref().into(),
                dcat_mqa::CONTAINS_QUALITY_ANNOTATION,
                five_star_annotation.as_ref().into(),
                output_store,
            )?;
        }
        None => tracing::warn!("Could not find five-star-annotation"),
    }

    Ok(())
}

async fn calculate_distribution_metrics(
    dist_assessment_node: NamedNodeRef<'_>,
    dist_node: NamedNodeRef<'_>,
    store: &Store,
    metrics_store: &Store,
) -> Result<(), Error> {
    add_availability_measurements(
        DISTRIBUTION_AVAILABILITY_CHECKS,
        dist_assessment_node,
        dist_node,
        store,
        metrics_store,
    )?;

    let format_result = check_format_and_media_type_alignment(
        dist_assessment_node,
        dist_node,
        store,
        metrics_store,
    )
    .await?;
    let license_result =
        check_license_metrics(dist_assessment_node, dist_node, store, metrics_store).await?;

    attach_five_star_rating(
        dist_assessment_node,
        dist_node,
        metrics_store,
        &FiveStarInputs {
            is_open_license: license_result.is_open_license,
            is_format_machine_interpretable: format_result.is_format_machine_interpretable,
            is_format_non_proprietary: format_result.is_format_non_proprietary,
            is_format_rdf: format_result.is_format_rdf,
            has_linked_resources: false,
        },
        &FiveStarDerivedFrom {
            open_license: license_result.open_license_derived_from,
            machine_interpretable: format_result.machine_interpretable_derived_from,
            non_proprietary: format_result.non_proprietary_derived_from,
        },
    )?;

    Ok(())
}

async fn check_format_and_media_type_alignment(
    dist_assessment_node: NamedNodeRef<'_>,
    dist_node: NamedNodeRef<'_>,
    store: &Store,
    metrics_store: &Store,
) -> Result<FormatCheckResult, Error> {
    // Machine-interpretable and non-proprietary checks not yet implemented.
    let is_format_machine_interpretable = false;
    let is_format_non_proprietary = false;
    let mut is_format_rdf = false;
    let mut is_format_aligned = false;
    let mut is_media_type_aligned = false;
    let mut machine_interpretable_derived_from = None;
    let mut non_proprietary_derived_from = None;

    let has_format_property = has_property(dist_node.into(), dcterms::FORMAT, store);
    let has_media_type_property = has_property(dist_node.into(), dcat::MEDIA_TYPE, store);

    if has_format_property {
        is_format_aligned = any_valid_in_reference_data(
            list_format_uris(dist_node, store),
            valid_format_or_media_type,
        )
        .await;

        if is_format_aligned {
            is_format_rdf = formats_include_rdf(dist_node, store);

            machine_interpretable_derived_from = Some(add_quality_measurement(
                dcat_mqa::FORMAT_MEDIA_TYPE_MACHINE_INTERPRETABLE,
                dist_assessment_node,
                dist_node.into(),
                is_format_machine_interpretable,
                metrics_store,
            )?);

            non_proprietary_derived_from = Some(add_quality_measurement(
                dcat_mqa::FORMAT_MEDIA_TYPE_NON_PROPRIETARY,
                dist_assessment_node,
                dist_node.into(),
                is_format_non_proprietary,
                metrics_store,
            )?);
        }
    }

    if has_media_type_property {
        is_media_type_aligned = any_valid_in_reference_data(
            list_media_type_uris(dist_node, store),
            valid_format_or_media_type,
        )
        .await;
    }

    add_quality_measurement(
        dcat_mqa::FORMAT_MEDIA_TYPE_VOCABULARY_ALIGNMENT,
        dist_assessment_node,
        dist_node.into(),
        is_format_aligned || is_media_type_aligned,
        metrics_store,
    )?;

    Ok(FormatCheckResult {
        is_format_machine_interpretable,
        is_format_non_proprietary,
        is_format_rdf,
        machine_interpretable_derived_from,
        non_proprietary_derived_from,
    })
}

async fn check_license_metrics(
    dist_assessment_node: NamedNodeRef<'_>,
    dist_node: NamedNodeRef<'_>,
    store: &Store,
    metrics_store: &Store,
) -> Result<LicenseCheckResult, Error> {
    let mut is_open_license = false;
    let mut open_license_derived_from = None;

    let has_license_property = has_property(dist_node.into(), dcterms::LICENSE, store);

    if has_license_property {
        is_open_license =
            any_valid_in_reference_data(list_license_uris(dist_node, store), valid_open_license)
                .await;

        add_quality_measurement(
            dcat_mqa::KNOWN_LICENSE,
            dist_assessment_node,
            dist_node.into(),
            is_open_license,
            metrics_store,
        )?;

        open_license_derived_from = Some(add_quality_measurement(
            dcat_mqa::OPEN_LICENSE,
            dist_assessment_node,
            dist_node.into(),
            is_open_license,
            metrics_store,
        )?);
    }

    Ok(LicenseCheckResult {
        is_open_license,
        open_license_derived_from,
    })
}

#[cfg(test)]
mod tests {
    use crate::vocab::{dcat_mqa, dqv};

    use super::*;
    use oxigraph::model::{vocab, Literal, NamedNodeRef, NamedOrBlankNode};
    use std::env;
    use tokio::runtime::Runtime;

    fn boolean_literal(expected: bool) -> Term {
        Term::Literal(Literal::new_typed_literal(
            if expected { "true" } else { "false" },
            NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean"),
        ))
    }

    fn set_reference_data_base_url(server: &httpmock::MockServer) {
        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );
    }

    fn setup_reference_data_mock() -> httpmock::MockServer {
        let server = httpmock::MockServer::start();

        server.mock(|when, then| {
            when.path("/reference-data/iana/media-types");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "mediaTypes": [
                            {"uri":"https://www.iana.org/assignments/media-types/text/csv","name":"csv","type":"text","subType":"csv"},
                            {"uri":"https://www.iana.org/assignments/media-types/text/csv-schema","name":"csv-schema","type":"text","subType":"csv-schema"}
                        ]
                    }
                "#,
                );
        });

        server.mock(|when, then| {
            when.path("/reference-data/eu/file-types");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "fileTypes": [
                            {"uri":"http://publications.europa.eu/resource/authority/file-type/7Z","code":"7Z","mediaType":"application/x-7z-compressed"}
                        ]
                    }
                "#,
                );
        });

        server.mock(|when, then| {
            when.path("/reference-data/open-licenses");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "openLicenses":[
                            {"uri":"http://creativecommons.org/licenses/by/4.0/","code":"CC BY 4.0","label":{"no":"Creative Commons Navngivelse 4.0 Internasjonal","en":"Creative Commons Attribution 4.0 International"}},{"uri":"http://creativecommons.org/licenses/by/4.0/deed.no","code":"CC BY 4.0 DEED","isReplacedBy":"http://creativecommons.org/licenses/by/4.0/","label":{"no":"Creative Commons Navngivelse 4.0 Internasjonal","en":"Creative Commons Attribution 4.0 International"}},{"uri":"http://creativecommons.org/publicdomain/zero/1.0/","code":"CC0 1.0","label":{"no":"Creative Commons Universal Fristatus-erklæring","en":"Creative Commons Universal Public Domain Dedication"}},{"uri":"http://data.norge.no/nlod/","code":"NLOD","isReplacedBy":"http://data.norge.no/nlod/no/2.0","label":{"no":"Norsk lisens for offentlige data","en":"Norwegian Licence for Open Government Data"}},{"uri":"http://data.norge.no/nlod/no/","code":"NLOD","isReplacedBy":"http://data.norge.no/nlod/no/2.0","label":{"no":"Norsk lisens for offentlige data","en":"Norwegian Licence for Open Government Data"}},{"uri":"http://data.norge.no/nlod/no/1.0","code":"NLOD10","isReplacedBy":"http://data.norge.no/nlod/no/2.0","label":{"no":"Norsk lisens for offentlige data","en":"Norwegian Licence for Open Government Data"}},{"uri":"http://data.norge.no/nlod/no/2.0","code":"NLOD20","label":{"no":"Norsk lisens for offentlige data","en":"Norwegian Licence for Open Government Data"}},{"uri":"http://publications.europa.eu/resource/authority/licence/NLOD_2_0","code":"NLOD_2_0","label":{"no":"Norsk lisens for offentlige data","en":"Norwegian Licence for Open Government Data"}}
                        ]
                    }
                "#,
                );
        });

        server.mock(|when, then| {
            when.path("/reference-data/eu/access-rights");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "accessRights":[
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/CONFIDENTIAL","code":"CONFIDENTIAL","label":{"en":"confidential"}},
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/NON_PUBLIC","code":"NON_PUBLIC","label":{"en":"non-public"}},
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/NORMAL","code":"NORMAL","label":{"en":"normal"}},
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/PUBLIC","code":"PUBLIC","label":{"en":"public"}},
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/RESTRICTED","code":"RESTRICTED","label":{"en":"restricted"}},
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/SENSITIVE","code":"SENSITIVE","label":{"en":"sensitive"}}
                        ]
                    }
                "#,
                );
        });

        server
    }

    fn setup_access_rights_mock() -> httpmock::MockServer {
        let server = httpmock::MockServer::start();

        server.mock(|when, then| {
            when.path("/reference-data/eu/access-rights");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "accessRights":[
                            {"uri":"http://publications.europa.eu/resource/authority/access-right/PUBLIC","code":"PUBLIC","label":{"en":"public"}}
                        ]
                    }
                "#,
                );
        });

        server
    }

    fn run_metrics_on_ttl(input: &str) -> String {
        Runtime::new()
            .unwrap()
            .block_on(parse_rdf_graph_and_calculate_metrics(
                &Store::new().unwrap(),
                &Store::new().unwrap(),
                input.to_string(),
            ))
            .unwrap()
    }

    fn assert_measurement_value(store: &Store, metric: NamedNodeRef, expected: bool) {
        let measurement = store
            .quads_for_pattern(None, None, Some(metric.into()), None)
            .next()
            .unwrap_or_else(|| panic!("Measurement not found for {}", metric.as_str()))
            .unwrap();

        let value_quad = store
            .quads_for_pattern(
                Some(measurement.subject.as_ref()),
                Some(dqv::VALUE.into()),
                None,
                None,
            )
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(value_quad.object, boolean_literal(expected));
    }

    #[test]
    fn test_determine_star_rating() {
        assert_eq!(
            determine_star_rating(&FiveStarInputs {
                is_open_license: false,
                is_format_machine_interpretable: false,
                is_format_non_proprietary: false,
                is_format_rdf: false,
                has_linked_resources: false,
            }),
            dcat_mqa::ZERO_STARS
        );

        assert_eq!(
            determine_star_rating(&FiveStarInputs {
                is_open_license: true,
                is_format_machine_interpretable: false,
                is_format_non_proprietary: false,
                is_format_rdf: false,
                has_linked_resources: false,
            }),
            dcat_mqa::ONE_STAR
        );

        assert_eq!(
            determine_star_rating(&FiveStarInputs {
                is_open_license: true,
                is_format_machine_interpretable: true,
                is_format_non_proprietary: false,
                is_format_rdf: false,
                has_linked_resources: false,
            }),
            dcat_mqa::TWO_STARS
        );

        assert_eq!(
            determine_star_rating(&FiveStarInputs {
                is_open_license: true,
                is_format_machine_interpretable: true,
                is_format_non_proprietary: true,
                is_format_rdf: false,
                has_linked_resources: false,
            }),
            dcat_mqa::THREE_STARS
        );

        assert_eq!(
            determine_star_rating(&FiveStarInputs {
                is_open_license: true,
                is_format_machine_interpretable: true,
                is_format_non_proprietary: true,
                is_format_rdf: true,
                has_linked_resources: false,
            }),
            dcat_mqa::FOUR_STARS
        );

        assert_eq!(
            determine_star_rating(&FiveStarInputs {
                is_open_license: true,
                is_format_machine_interpretable: true,
                is_format_non_proprietary: true,
                is_format_rdf: true,
                has_linked_resources: true,
            }),
            dcat_mqa::FIVE_STARS
        );
    }

    #[test]
    fn test_parse_graph_and_collect_metrics() {
        let server = setup_reference_data_mock();
        set_reference_data_base_url(&server);

        let mqa_graph = run_metrics_on_ttl(include_str!("../tests/data/dataset_event.ttl"));

        let store_expected = Store::new().unwrap();
        parse_turtle(
            &store_expected,
            include_str!("../tests/data/mqa_event.ttl").to_string(),
        )
        .unwrap();

        let store_actual = Store::new().unwrap();
        parse_turtle(&store_actual, mqa_graph).unwrap();
        assert_eq!(
            store_expected
                .quads_for_pattern(None, None, None, None)
                .count(),
            store_actual
                .quads_for_pattern(None, None, None, None)
                .count()
        );

        let dataset_assessment = store_actual
            .quads_for_pattern(
                None,
                Some(vocab::rdf::TYPE),
                Some(dcat_mqa::DATASET_ASSESSMENT_CLASS.into()),
                None,
            )
            .next()
            .and_then(|d| match d {
                Ok(Quad {
                    subject: NamedOrBlankNode::NamedNode(s),
                    ..
                }) => Some(s),
                _ => None,
            })
            .unwrap();

        assert_eq!(
            1,
            store_actual
                .quads_for_pattern(
                    Some(dataset_assessment.as_ref().into()),
                    Some(dcat_mqa::CONTAINS_QUALITY_ANNOTATION),
                    None,
                    None
                )
                .count()
        );

        assert_eq!(
            10,
            store_actual
                .quads_for_pattern(
                    Some(dataset_assessment.as_ref().into()),
                    Some(dcat_mqa::CONTAINS_QUALITY_MEASUREMENT),
                    None,
                    None
                )
                .count()
        );

        let dist_assessment_quad = store_actual
            .quads_for_pattern(
                Some(dataset_assessment.as_ref().into()),
                Some(dcat_mqa::HAS_DISTRIBUTION_ASSESSMENT),
                None,
                None,
            )
            .next()
            .unwrap()
            .unwrap();

        if let Term::NamedNode(node) = dist_assessment_quad.object.clone() {
            assert_eq!(
                14,
                store_actual
                    .quads_for_pattern(
                        Some(node.as_ref().into()),
                        Some(dcat_mqa::CONTAINS_QUALITY_MEASUREMENT),
                        None,
                        None
                    )
                    .count()
            );

            assert_measurement_value(&store_actual, dcat_mqa::KNOWN_LICENSE, true);
        } else {
            panic!("Distribution assessment is not a named node")
        };

        env::remove_var("REFERENCE_DATA_BASE_URL");
    }

    #[test]
    fn test_access_rights_vocabulary_alignment_valid() {
        let server = setup_access_rights_mock();
        set_reference_data_base_url(&server);

        let input_ttl = r#"
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.com/dataset> rdf:type dcat:Dataset ;
    dcatnomqa:hasAssessment <http://dataset.assessment.no> ;
    dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> .
"#;

        let mqa_graph = run_metrics_on_ttl(input_ttl);

        let store = Store::new().unwrap();
        parse_turtle(&store, mqa_graph).unwrap();

        assert_measurement_value(&store, dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT, true);

        env::remove_var("REFERENCE_DATA_BASE_URL");
    }

    #[test]
    fn test_access_rights_vocabulary_alignment_invalid() {
        let server = setup_access_rights_mock();
        set_reference_data_base_url(&server);

        let input_ttl = r#"
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.com/dataset> rdf:type dcat:Dataset ;
    dcatnomqa:hasAssessment <http://dataset.assessment.no> ;
    dct:accessRights <http://example.com/invalid-access-right> .
"#;

        let mqa_graph = run_metrics_on_ttl(input_ttl);

        let store = Store::new().unwrap();
        parse_turtle(&store, mqa_graph).unwrap();

        assert_measurement_value(&store, dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT, false);

        env::remove_var("REFERENCE_DATA_BASE_URL");
    }

    #[test]
    fn test_access_rights_vocabulary_alignment_no_access_rights() {
        let server = setup_access_rights_mock();
        set_reference_data_base_url(&server);

        let input_ttl = r#"
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.com/dataset> rdf:type dcat:Dataset ;
    dcatnomqa:hasAssessment <http://dataset.assessment.no> .
"#;

        let mqa_graph = run_metrics_on_ttl(input_ttl);

        let store = Store::new().unwrap();
        parse_turtle(&store, mqa_graph).unwrap();

        assert_measurement_value(&store, dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT, false);

        env::remove_var("REFERENCE_DATA_BASE_URL");
    }
}
