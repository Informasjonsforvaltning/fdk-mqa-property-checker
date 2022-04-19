use std::str;

use chrono::Utc;

use log::error;

use oxigraph::model::*;
use oxigraph::store::{StorageError, Store};

use crate::schemas::{MQAEvent, MQAEventType};

use crate::reference_data::{valid_file_type, valid_media_type};

use crate::vocab::{dcat, dcat_mqa, dcterms, oa};

use crate::rdf::{
    add_derived_from, add_five_star_annotation, add_property, add_quality_measurement,
    convert_term_to_named_or_blank_node_ref, create_metrics_store, dump_graph_as_turtle,
    get_dataset_node, has_property, is_rdf_format, list_distributions, list_formats,
    list_media_types, parse_turtle,
};

pub fn parse_rdf_graph_and_calculate_metrics(
    fdk_id: String,
    graph: String,
) -> Result<MQAEvent, String> {
    match parse_turtle(graph) {
        Ok(store) => {
            match get_dataset_node(&store) {
                Some(dataset_node) => {
                    match calculate_metrics(dataset_node.as_ref(), &store) {
                        Ok(metrics_store) => {
                            match dump_graph_as_turtle(&metrics_store) {
                                Ok(bytes) => {
                                    // Create MQA event
                                    match str::from_utf8(bytes.as_slice()) {
                                        Ok(turtle) => Ok(MQAEvent {
                                            event_type: MQAEventType::PropertiesChecked,
                                            fdk_id,
                                            graph: turtle.to_string(),
                                            timestamp: Utc::now().timestamp_millis(),
                                        }),
                                        Err(e) => {
                                            Err(format!("Failed dumping graph as turtle: {}", e))
                                        }
                                    }
                                }
                                Err(e) => Err(format!("Failed dumping graph as turtle: {}", e)),
                            }
                        }
                        Err(e) => Err(format!("{}", e)),
                    }
                }
                None => Err(format!("{} - Dataset node not found in graph", fdk_id)),
            }
        }
        Err(e) => Err(format!("{}", e)),
    }
}

fn calculate_metrics(dataset_node: NamedNodeRef, store: &Store) -> Result<Store, StorageError> {
    // Make MQA metrics model (DQV)
    let metrics_store = create_metrics_store(dataset_node)?;

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
            dataset_node.into(),
            props
                .into_iter()
                .any(|p| has_property(dataset_node.into(), p, &store)),
            &metrics_store,
        )?;
    }

    // TODO Verify if valid license uri
    add_quality_measurement(
        dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT,
        dataset_node.into(),
        false,
        &metrics_store,
    )?;

    for quad in list_distributions(dataset_node, &store) {
        match quad {
            Ok(dist_quad) => {
                metrics_store.insert(dist_quad.as_ref())?;
                match convert_term_to_named_or_blank_node_ref(dist_quad.object.as_ref()) {
                    Some(dist_node) => {
                        calculate_distribution_metrics(dist_node, store, &metrics_store)?;
                    }
                    None => error!(
                        "Distribution is not a named or blank node {}",
                        dist_quad.object
                    ),
                }
            }
            Err(e) => error!("Listing distributions failed {}", e),
        }
    }

    Ok(metrics_store)
}

fn calculate_distribution_metrics(
    dist_node: NamedOrBlankNodeRef,
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

    if has_format_property {
        is_format_aligned = list_formats(dist_node, &store).any(|mt| match mt {
            Ok(Quad {
                object: Term::NamedNode(nn),
                ..
            }) => {
                valid_file_type(nn.as_str().to_string())
                    || valid_media_type(nn.as_str().to_string())
            }
            _ => false,
        });

        if is_format_aligned {
            is_format_rdf = list_formats(dist_node, &store).any(|mt| match mt {
                Ok(Quad {
                    object: Term::NamedNode(nn),
                    ..
                }) => is_rdf_format(nn.as_str()),
                _ => false,
            });

            // TODO check reference data
            is_format_machine_interpretable = false;
            is_format_non_proprietary = false;

            five_star_machine_interpretable_derived_from = Some(add_quality_measurement(
                dcat_mqa::FORMAT_MEDIA_TYPE_MACHINE_INTERPRETABLE,
                dist_node.into(),
                is_format_machine_interpretable,
                &metrics_store,
            )?);

            five_star_non_proprietary_derived_from = Some(add_quality_measurement(
                dcat_mqa::FORMAT_MEDIA_TYPE_NON_PROPRIETARY,
                dist_node.into(),
                is_format_non_proprietary,
                &metrics_store,
            )?);
        }
    }

    if has_media_type_property {
        is_media_type_aligned = list_media_types(dist_node, &store).any(|mt| match mt {
            Ok(Quad {
                object: Term::NamedNode(nn),
                ..
            }) => {
                valid_file_type(nn.as_str().to_string())
                    || valid_media_type(nn.as_str().to_string())
            }
            _ => false,
        });
    }

    add_quality_measurement(
        dcat_mqa::FORMAT_MEDIA_TYPE_VOCABULARY_ALIGNMENT,
        dist_node.into(),
        is_format_aligned || is_media_type_aligned,
        &metrics_store,
    )?;

    if has_license_property {
        // TODO
        add_quality_measurement(
            dcat_mqa::KNOWN_LICENSE,
            dist_node.into(),
            false,
            &metrics_store,
        )?;

        // TODO
        five_star_open_license_derived_from = Some(add_quality_measurement(
            dcat_mqa::OPEN_LICENSE,
            dist_node.into(),
            false,
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
    use super::*;
    use std::env;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        let server = httpmock::MockServer::start();

        server.mock(|when, then| {
            when.path("/iana/media-types");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "mediaTypes": []
                    }
                "#,
                );
        });

        server.mock(|when, then| {
            when.path("/eu/file-types");
            then.status(200)
                .header("content-type", "application/json")
                .body(
                    r#"
                    {
                        "fileTypes": []
                    }
                "#,
                );
        });

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let mqa_event = parse_rdf_graph_and_calculate_metrics("1".to_string(), r#"
            @prefix adms: <http://www.w3.org/ns/adms#> . 
            @prefix cpsv: <http://purl.org/vocab/cpsv#> . 
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> . 
            @prefix dcat: <http://www.w3.org/ns/dcat#> . 
            @prefix dct: <http://purl.org/dc/terms/> . 
            @prefix dqv: <http://www.w3.org/ns/dqv#> . 
            @prefix eli: <http://data.europa.eu/eli/ontology#> . 
            @prefix foaf: <http://xmlns.com/foaf/0.1/> . 
            @prefix iso: <http://iso.org/25012/2008/dataquality/> . 
            @prefix oa: <http://www.w3.org/ns/oa#> . 
            @prefix prov: <http://www.w3.org/ns/prov#> . 
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . 
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . 
            @prefix schema: <http://schema.org/> . 
            @prefix skos: <http://www.w3.org/2004/02/skos/core#> . 
            @prefix vcard: <http://www.w3.org/2006/vcard/ns#> . 
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . 
            
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> rdf:type dcat:Dataset ; 
                dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> ; 
                dct:description "Visning over all norsk offentlig bistand fra 1960 til siste kalenderår sortert etter partnerorganisasjoner."@nb ; 
                dct:identifier "https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572" ; 
                dct:language <http://publications.europa.eu/resource/authority/language/NOR> , <http://publications.europa.eu/resource/authority/language/ENG> ; 
                dct:provenance <http://data.brreg.no/datakatalog/provinens/nasjonal> ; 
                dct:publisher <https://organization-catalogue.fellesdatakatalog.digdir.no/organizations/971277882> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dct:type "Data" ; 
                dcat:contactPoint [ rdf:type vcard:Organization ; vcard:hasEmail <mailto:resultater@norad.no> ] ; 
                dcat:distribution [ 
                    rdf:type dcat:Distribution ; dct:description "Norsk bistand i tall etter partner"@nb ; 
                    dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , 
                            <https://www.iana.org/assignments/media-types/text/csv> ; 
                    dct:license <http://data.norge.no/nlod/no/2.0> ; 
                    dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                    dcat:accessURL <https://resultater.norad.no/partner/> ] ; 
                dcat:keyword "oda"@nb , "norad"@nb , "bistand"@nb ; 
                dcat:landingPage <https://resultater.norad.no/partner/> ; 
                dcat:theme <http://publications.europa.eu/resource/authority/data-theme/INTR> ; 
                dqv:hasQualityAnnotation [ rdf:type dqv:QualityAnnotation ; dqv:inDimension iso:Currentness ] ; 
                prov:qualifiedAttribution [ 
                    rdf:type prov:Attribution ; 
                    dcat:hadRole <http://registry.it.csiro.au/def/isotc211/CI_RoleCode/contributor> ; 
                    prov:agent <https://data.brreg.no/enhetsregisteret/api/enheter/971277882> ] . 
                <http://publications.europa.eu/resource/authority/language/ENG> rdf:type dct:LinguisticSystem ; 
                    <http://publications.europa.eu/ontology/authority/authority-code> "ENG" ; 
                    skos:prefLabel "Engelsk"@nb . 
                <http://publications.europa.eu/resource/authority/language/NOR> rdf:type dct:LinguisticSystem ; 
                    <http://publications.europa.eu/ontology/authority/authority-code> "NOR" ; skos:prefLabel "Norsk"@nb .
        "#.to_string());

        let store_expected = parse_turtle(String::from(
            r#"<https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:2e0587e7a28b492755a38437372b2e05 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:38fc04f528a7eef5b4102f9fdd4b9ab6 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:5cead1a2399fcb8ea6ec957254ddf186 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:6fa77fe6d9fe5abd71949e9b74f63a46 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:9919fee0b16fa958dbc231c6f1f542d4 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:9e90199079487760d26f4e022db8c116 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:c0cc2452ef89d2b1343d07254497828e .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f0c942556bf9c7b4ddc968bcef39b6f4 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f991bd5d3daf2b0b894775e0797afeea .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:fd38f82c4726d61ffd3920fd165ba303 .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
        <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dcat#distribution> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#licenseAvailability> .
        _:972515fe91764948597fbb3beebedc5 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#openLicense> .
        _:17a511e66065f4607ba5bdb4a89bd2ee <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#categoryAvailability> .
        _:2e0587e7a28b492755a38437372b2e05 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
        _:38fc04f528a7eef5b4102f9fdd4b9ab6 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#spatialAvailability> .
        _:5cead1a2399fcb8ea6ec957254ddf186 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsAvailability> .
        _:6fa77fe6d9fe5abd71949e9b74f63a46 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
        _:75039bd0fdf7843c5441c5807a4ec42f <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#knownLicense> .
        _:832a54ed610f7d5636eb4c42a8ebfcd7 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#mediaTypeAvailability> .
        _:88429707f7d93b283ba7f140c12044fe <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
        _:8b20c408a89600e4c506d8ad0e0f4ef2 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityAnnotation> .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/ns/oa#hasBody> <https://data.norge.no/vocabulary/dcatno-mqa#zeroStars> .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#classifying> .
        _:91a3e690dbbcf753008d6d1836be234e <http://www.w3.org/ns/prov#wasDerivedFrom> _:17a511e66065f4607ba5bdb4a89bd2ee .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#rightsAvailability> .
        _:95261ca4b6eb5455fb9222dbc9481ee1 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#publisherAvailability> .
        _:9919fee0b16fa958dbc231c6f1f542d4 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsVocabularyAlignment> .
        _:9e90199079487760d26f4e022db8c116 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#byteSizeAvailability> .
        _:a28c8063eb23a04eb056ed77af71714a <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars> .
        _:ac9b2d402b7da13f8ee4d49df729d93e <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability> .
        _:c0cc2452ef89d2b1343d07254497828e <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
        _:e2e93b98661a6f50e837434ae104a538 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeVocabularyAlignment> .
        _:ed65dfa5fa665e84b15bc107d9ccf087 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
        _:f014b40cce0afd210f34b97cf54e0a50 <http://www.w3.org/ns/dqv#computedOn> _:f9b4fdb9378aa7013a762790b069eb7e .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#temporalAvailability> .
        _:f0c942556bf9c7b4ddc968bcef39b6f4 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#contactPointAvailability> .
        _:f991bd5d3daf2b0b894775e0797afeea <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:972515fe91764948597fbb3beebedc5 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:17a511e66065f4607ba5bdb4a89bd2ee .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:75039bd0fdf7843c5441c5807a4ec42f .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:832a54ed610f7d5636eb4c42a8ebfcd7 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:88429707f7d93b283ba7f140c12044fe .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:8b20c408a89600e4c506d8ad0e0f4ef2 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:95261ca4b6eb5455fb9222dbc9481ee1 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a28c8063eb23a04eb056ed77af71714a .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:ac9b2d402b7da13f8ee4d49df729d93e .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:e2e93b98661a6f50e837434ae104a538 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:ed65dfa5fa665e84b15bc107d9ccf087 .
        _:f9b4fdb9378aa7013a762790b069eb7e <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f014b40cce0afd210f34b97cf54e0a50 .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
        _:fd38f82c4726d61ffd3920fd165ba303 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> ."#,
        )).unwrap();

        assert!(mqa_event.is_ok());
        let store_actual = parse_turtle(mqa_event.unwrap().graph).unwrap();
        assert_eq!(
            store_expected
                .quads_for_pattern(None, None, None, None)
                .count(),
            store_actual
                .quads_for_pattern(None, None, None, None)
                .count()
        );
    }
}
