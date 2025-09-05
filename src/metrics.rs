use futures::StreamExt;
use oxigraph::{
    model::{BlankNode, NamedNodeRef, Quad, Term},
    store::{StorageError, Store},
};
use crate::{
    error::Error,
    rdf::{
        add_derived_from, add_five_star_annotation, add_property, add_quality_measurement,
        dump_graph_as_turtle, get_dataset_node, get_five_star_annotation, has_property,
        insert_dataset_assessment, insert_distribution_assessment, is_rdf_format,
        list_access_rights, list_distributions, list_formats, list_licenses, list_media_types, node_assessment,
        parse_turtle,
    },
    reference_data::{valid_file_type, valid_media_type, valid_open_license, valid_access_right},
    vocab::{dcat, dcat_mqa, dcterms, oa},
};

pub async fn parse_rdf_graph_and_calculate_metrics(
    input_store: &Store,
    output_store: &Store,
    graph: String,
) -> Result<String, Error> {
    parse_turtle(input_store, graph)?;
    let dataset_node = get_dataset_node(input_store).ok_or("Dataset node not found in graph")?;
    let _ = calculate_metrics(dataset_node.as_ref(), input_store, output_store).await;
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

    for (metric, props) in vec![
        (
            dcat_mqa::ACCESS_RIGHTS_AVAILABILITY,
            vec![dcterms::ACCESS_RIGHTS],
        ),
        (dcat_mqa::CATEGORY_AVAILABILITY, vec![dcat::THEME]),
        (
            dcat_mqa::CONTACT_POINT_AVAILABILITY,
            vec![dcat::CONTACT_POINT],
        ),
        (
            dcat_mqa::KEYWORD_AVAILABILITY,
            vec![dcat::KEYWORD, dcterms::SUBJECT],
        ),
        (dcat_mqa::PUBLISHER_AVAILABILITY, vec![dcterms::PUBLISHER]),
        (dcat_mqa::SPATIAL_AVAILABILITY, vec![dcterms::SPATIAL]),
        (dcat_mqa::TEMPORAL_AVAILABILITY, vec![dcterms::TEMPORAL]),
        (dcat_mqa::DATE_ISSUED_AVAILABILITY, vec![dcterms::ISSUED]),
        (
            dcat_mqa::DATE_MODIFIED_AVAILABILITY,
            vec![dcterms::MODIFIED],
        ),
    ] {
        add_quality_measurement(
            metric,
            dataset_assessment.as_ref(),
            dataset_node.into(),
            props
                .into_iter()
                .any(|p| has_property(dataset_node.into(), p, input_store)),
            &output_store,
        )?;
    }

    // Check if access rights align with controlled vocabulary
    let mut access_rights: Vec<String> = Vec::new();
    list_access_rights(dataset_node, input_store).for_each(|ar| match ar {
        Ok(Quad {
               object: Term::NamedNode(nn),
               ..
           }) => access_rights.push(nn.as_str().to_string()),
        _ => {},
    });

    let has_access_rights_property = has_property(dataset_node.into(), dcterms::ACCESS_RIGHTS, input_store);
    let is_access_rights_aligned = if has_access_rights_property {
        futures::stream::iter(access_rights)
            .any(|access_right| async move {
                valid_access_right(access_right.to_string()).await
            }).await
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
        ).await?;
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
) -> Result<(), StorageError> {
    for (metric, props) in vec![
        (dcat_mqa::BYTE_SIZE_AVAILABILITY, vec![dcat::BYTE_SIZE]),
        (dcat_mqa::DATE_ISSUED_AVAILABILITY, vec![dcterms::ISSUED]),
        (
            dcat_mqa::DATE_MODIFIED_AVAILABILITY,
            vec![dcterms::MODIFIED],
        ),
        (
            dcat_mqa::DOWNLOAD_URL_AVAILABILITY,
            vec![dcat::DOWNLOAD_URL],
        ),
        (dcat_mqa::RIGHTS_AVAILABILITY, vec![dcterms::RIGHTS]),
        (dcat_mqa::FORMAT_AVAILABILITY, vec![dcterms::FORMAT]),
        (dcat_mqa::LICENSE_AVAILABILITY, vec![dcterms::LICENSE]),
        (dcat_mqa::MEDIA_TYPE_AVAILABILITY, vec![dcat::MEDIA_TYPE]),
    ] {
        add_quality_measurement(
            metric,
            dist_assessment_node,
            dist_node.into(),
            props
                .into_iter()
                .any(|p| has_property(dist_node.into(), p, &store)),
            &metrics_store,
        )?;
    }

    let mut five_star_open_license_derived_from: Option<BlankNode> = None;
    let mut five_star_machine_interpretable_derived_from: Option<BlankNode> = None;
    let mut five_star_non_proprietary_derived_from: Option<BlankNode> = None;

    let has_open_license = false;
    let mut is_format_aligned = false;
    let mut is_format_machine_interpretable = false;
    let mut is_format_non_proprietary = false;
    let mut is_format_rdf = false;
    let mut is_media_type_aligned = false;
    // Currently not possible to check this!
    let has_linked_recourses = false;

    let has_format_property = has_property(dist_node.into(), dcterms::FORMAT, &store);
    let has_media_type_property = has_property(dist_node.into(), dcat::MEDIA_TYPE, &store);
    let has_license_property = has_property(dist_node.into(), dcterms::LICENSE, &store);

    let mut formats: Vec<String> = Vec::new();
    list_formats(dist_node, &store).for_each(|mt| match mt {
        Ok(Quad {
               object: Term::NamedNode(nn),
               ..
           }) => formats.push(nn.as_str().to_string()),
        _ => {},
    });

    if has_format_property {
        is_format_aligned = futures::stream::iter(formats)
            .any(|format| async move {
                valid_file_type(format.to_string()).await
                    || valid_media_type(format.to_string()).await
            })
            .await;

        if is_format_aligned {
            is_format_rdf = list_formats(dist_node, &store).any(|mt| match mt {
                Ok(Quad {
                    object: Term::NamedNode(nn),
                    ..
                }) => is_rdf_format(nn.as_str()),
                _ => false,
            });

            is_format_machine_interpretable = false;
            is_format_non_proprietary = false;

            five_star_machine_interpretable_derived_from = Some(add_quality_measurement(
                dcat_mqa::FORMAT_MEDIA_TYPE_MACHINE_INTERPRETABLE,
                dist_assessment_node,
                dist_node.into(),
                is_format_machine_interpretable,
                &metrics_store,
            )?);

            five_star_non_proprietary_derived_from = Some(add_quality_measurement(
                dcat_mqa::FORMAT_MEDIA_TYPE_NON_PROPRIETARY,
                dist_assessment_node,
                dist_node.into(),
                is_format_non_proprietary,
                &metrics_store,
            )?);
        }
    }

    let mut media_types: Vec<String> = Vec::new();
    list_media_types(dist_node, &store).for_each(|mt| match mt {
        Ok(Quad {
               object: Term::NamedNode(nn),
               ..
           }) => media_types.push(nn.as_str().to_string()),
        _ => {},
    });

    if has_media_type_property {
        is_media_type_aligned = futures::stream::iter(media_types)
            .any(|media_type| async move {
                valid_file_type(media_type.to_string()).await
                    || valid_media_type(media_type.to_string()).await
            }).await;
    }

    add_quality_measurement(
        dcat_mqa::FORMAT_MEDIA_TYPE_VOCABULARY_ALIGNMENT,
        dist_assessment_node,
        dist_node.into(),
        is_format_aligned || is_media_type_aligned,
        &metrics_store,
    )?;

    let mut licenses: Vec<String> = Vec::new();
    list_licenses(dist_node, &store).for_each(|mt| match mt {
        Ok(Quad {
               object: Term::NamedNode(nn),
               ..
           }) => licenses.push(nn.as_str().to_string()),
        _ => {},
    });

    if has_license_property {
        let is_open_license: bool = futures::stream::iter(licenses)
            .any(|license| async move {
                valid_open_license(license.to_string()).await
            }).await;

        add_quality_measurement(
            dcat_mqa::KNOWN_LICENSE,
            dist_assessment_node,
            dist_node.into(),
            is_open_license,
            &metrics_store,
        )?;

        // TODO
        five_star_open_license_derived_from = Some(add_quality_measurement(
            dcat_mqa::OPEN_LICENSE,
            dist_assessment_node,
            dist_node.into(),
            is_open_license,
            &metrics_store,
        )?);
    }

    let five_star_quality_annotation = add_five_star_annotation(&metrics_store)?;
    let five_star_rating;

    // 0-Star is derived from the open licence measurement
    if let Some(derived) = five_star_open_license_derived_from {
        add_derived_from(
            five_star_quality_annotation.as_ref().into(),
            derived.as_ref().into(),
            &metrics_store,
        )?;
    }

    if has_open_license {
        // 1-Star is derived from the machine-interpretability measurement
        if let Some(derived) = five_star_machine_interpretable_derived_from {
            add_derived_from(
                five_star_quality_annotation.as_ref().into(),
                derived.as_ref().into(),
                &metrics_store,
            )?;
        }

        if is_format_machine_interpretable {
            // 2-Star is derived from the non-proprietary measurement
            if let Some(derived) = five_star_non_proprietary_derived_from {
                add_derived_from(
                    five_star_quality_annotation.as_ref().into(),
                    derived.as_ref().into(),
                    &metrics_store,
                )?;
            }

            if is_format_non_proprietary {
                if is_format_rdf {
                    if has_linked_recourses {
                        // Currently not evaluated
                        five_star_rating = Some(dcat_mqa::FIVE_STARS);
                    } else {
                        five_star_rating = Some(dcat_mqa::FOUR_STARS);
                    }
                } else {
                    five_star_rating = Some(dcat_mqa::THREE_STARS);
                }
            } else {
                five_star_rating = Some(dcat_mqa::TWO_STARS);
            }
        } else {
            five_star_rating = Some(dcat_mqa::ONE_STAR);
        }
    } else {
        five_star_rating = Some(dcat_mqa::ZERO_STARS);
    }

    add_quality_measurement(
        dcat_mqa::AT_LEAST_FOUR_STARS,
        dist_assessment_node,
        dist_node.into(),
        five_star_rating == Some(dcat_mqa::FIVE_STARS)
            || five_star_rating == Some(dcat_mqa::FOUR_STARS),
        &metrics_store,
    )?;

    if let Some(rating) = five_star_rating {
        add_property(
            five_star_quality_annotation.as_ref().into(),
            oa::HAS_BODY,
            rating.into(),
            &metrics_store,
        )?;
    }

    add_property(
        five_star_quality_annotation.as_ref().into(),
        oa::MOTIVATED_BY,
        oa::CLASSIFYING.into(),
        &metrics_store,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::vocab::{dcat_mqa, dqv};

    use super::*;
    use oxigraph::model::{vocab, Literal, Subject};
    use std::env;
    use tokio::runtime::Runtime;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
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

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let mqa_graph = Runtime::new().unwrap().block_on(
            parse_rdf_graph_and_calculate_metrics(
                &mut Store::new().unwrap(),
                &mut Store::new().unwrap(),
                include_str!("../tests/data/dataset_event.ttl").to_string(),
            )
        )
        .unwrap();

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
                    subject: Subject::NamedNode(s),
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

            let known_license_assessment = store_actual
                .quads_for_pattern(None, None, Some(dcat_mqa::KNOWN_LICENSE.into()), None)
                .next()
                .unwrap()
                .unwrap()
                .subject;

            let known_license_value = store_actual
                .quads_for_pattern(
                    Some(known_license_assessment.as_ref()),
                    Some(dqv::VALUE),
                    None,
                    None,
                )
                .next()
                .unwrap()
                .unwrap();

            assert_eq!(
                known_license_value.object,
                Term::Literal(Literal::new_typed_literal(
                    "true",
                    NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
                ))
            );
        } else {
            panic!("Distribution assessment is not a named node")
        };

        // Clean up environment variable
        env::remove_var("REFERENCE_DATA_BASE_URL");
    }

    #[test]
    fn test_access_rights_vocabulary_alignment_valid() {
        let server = httpmock::MockServer::start();

        // Mock access rights endpoint
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

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let input_ttl = r#"
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.com/dataset> rdf:type dcat:Dataset ;
    dcatnomqa:hasAssessment <http://dataset.assessment.no> ;
    dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> .
"#;

        let mqa_graph = Runtime::new().unwrap().block_on(
            parse_rdf_graph_and_calculate_metrics(
                &mut Store::new().unwrap(),
                &mut Store::new().unwrap(),
                input_ttl.to_string(),
            )
        )
        .unwrap();

        let store = Store::new().unwrap();
        parse_turtle(&store, mqa_graph).unwrap();

        // Check that access rights vocabulary alignment is true
        let access_rights_measurement = store
            .quads_for_pattern(
                None,
                None,
                Some(dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT.into()),
                None,
            )
            .next()
            .expect("Access rights vocabulary alignment measurement not found")
            .unwrap();

        let value_quad = store
            .quads_for_pattern(
                Some(access_rights_measurement.subject.as_ref()),
                Some(dqv::VALUE.into()),
                None,
                None,
            )
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(
            value_quad.object,
            Term::Literal(Literal::new_typed_literal(
                "true",
                NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
            ))
        );

        // Clean up environment variable
        env::remove_var("REFERENCE_DATA_BASE_URL");
    }

    #[test]
    fn test_access_rights_vocabulary_alignment_invalid() {
        let server = httpmock::MockServer::start();

        // Mock access rights endpoint
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

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let input_ttl = r#"
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.com/dataset> rdf:type dcat:Dataset ;
    dcatnomqa:hasAssessment <http://dataset.assessment.no> ;
    dct:accessRights <http://example.com/invalid-access-right> .
"#;

        let mqa_graph = Runtime::new().unwrap().block_on(
            parse_rdf_graph_and_calculate_metrics(
                &mut Store::new().unwrap(),
                &mut Store::new().unwrap(),
                input_ttl.to_string(),
            )
        )
        .unwrap();

        let store = Store::new().unwrap();
        parse_turtle(&store, mqa_graph).unwrap();

        // Check that access rights vocabulary alignment is false
        let access_rights_measurement = store
            .quads_for_pattern(
                None,
                None,
                Some(dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT.into()),
                None,
            )
            .next()
            .expect("Access rights vocabulary alignment measurement not found")
            .unwrap();

        let value_quad = store
            .quads_for_pattern(
                Some(access_rights_measurement.subject.as_ref()),
                Some(dqv::VALUE.into()),
                None,
                None,
            )
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(
            value_quad.object,
            Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
            ))
        );

        // Clean up environment variable
        env::remove_var("REFERENCE_DATA_BASE_URL");
    }

    #[test]
    fn test_access_rights_vocabulary_alignment_no_access_rights() {
        let server = httpmock::MockServer::start();

        // Mock access rights endpoint
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

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let input_ttl = r#"
@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://example.com/dataset> rdf:type dcat:Dataset ;
    dcatnomqa:hasAssessment <http://dataset.assessment.no> .
"#;

        let mqa_graph = Runtime::new().unwrap().block_on(
            parse_rdf_graph_and_calculate_metrics(
                &mut Store::new().unwrap(),
                &mut Store::new().unwrap(),
                input_ttl.to_string(),
            )
        )
        .unwrap();

        let store = Store::new().unwrap();
        parse_turtle(&store, mqa_graph).unwrap();

        // Check that access rights vocabulary alignment is false when no access rights property exists
        let access_rights_measurement = store
            .quads_for_pattern(
                None,
                None,
                Some(dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT.into()),
                None,
            )
            .next()
            .expect("Access rights vocabulary alignment measurement not found")
            .unwrap();

        let value_quad = store
            .quads_for_pattern(
                Some(access_rights_measurement.subject.as_ref()),
                Some(dqv::VALUE.into()),
                None,
                None,
            )
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(
            value_quad.object,
            Term::Literal(Literal::new_typed_literal(
                "false",
                NamedNodeRef::new_unchecked("http://www.w3.org/2001/XMLSchema#boolean")
            ))
        );

        // Clean up environment variable
        env::remove_var("REFERENCE_DATA_BASE_URL");
    }
}
