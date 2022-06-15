use std::str;

use chrono::Utc;

use log::{error, warn};

use oxigraph::model::vocab::rdf;
use oxigraph::model::{BlankNode, NamedNodeRef, NamedOrBlankNodeRef, Quad, Term};
use oxigraph::store::{StorageError, Store};

use crate::schemas::{MQAEvent, MQAEventType};

use crate::reference_data::{valid_file_type, valid_media_type};

use crate::vocab::{dcat, dcat_mqa, dcterms, dqv, oa};

use crate::rdf::{
    add_derived_from, add_five_star_annotation, add_property, add_quality_measurement,
    convert_term_to_named_or_blank_node_ref, create_metrics_store, dump_graph_as_turtle,
    get_dataset_node, get_five_star_annotation, has_property, is_rdf_format, list_distributions,
    list_formats, list_media_types, parse_turtle,
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
                        add_property(
                            dist_node.into(),
                            rdf::TYPE.into(),
                            dcat::DISTRIBUTION_CLASS.into(),
                            &metrics_store,
                        )?;
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

    match get_five_star_annotation(&metrics_store) {
        Some(five_star_annotation) => {
            add_property(
                dataset_node.into(),
                dqv::HAS_QUALITY_ANNOTATION,
                five_star_annotation.as_ref().into(),
                &metrics_store,
            )?;
        }
        None => warn!("Could not find five-star-annotation"),
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
    use crate::{rdf, utils::setup_logger};

    use super::*;
    use std::env;

    #[test]
    fn test_parse_graph_anc_collect_metrics() {
        setup_logger(true, None);

        let server = httpmock::MockServer::start();

        server.mock(|when, then| {
            when.path("/iana/media-types");
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
            when.path("/eu/file-types");
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
            r#"<https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityAnnotation> _:655a2d603373caf2cfea3e26fde4697c .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:4b8b60fabbdc5137fc432ea73999f5d0 .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:5967523cf8203c6de1527a2df417f8e7 .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:69ef28f5acbde9c94f21a288c041715b .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:77c5880861e68ceccd97f43faf62df0d .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:8391f99de1a1465114fe249cb22fe350 .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:8e39b6cb3cb549181bf3c325f50e5dea .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:ca0550f63c24e83c345fb26ff70fc868 .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d24a3a2898694457ef5cd197ac98a446 .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d62ad04a6bb514efb0cc1673dd59221c .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:e7a8b5f28be58d559c99ac8f33d98c2c .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Dataset> .
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <http://www.w3.org/ns/dcat#distribution> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:1c1218bfecd7df884f256cbdcff24c8d <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:1c1218bfecd7df884f256cbdcff24c8d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:1c1218bfecd7df884f256cbdcff24c8d <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#mediaTypeAvailability> .
            _:1c1218bfecd7df884f256cbdcff24c8d <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:232150fee40e4bcdc92b65b54f948654 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:232150fee40e4bcdc92b65b54f948654 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:232150fee40e4bcdc92b65b54f948654 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#licenseAvailability> .
            _:232150fee40e4bcdc92b65b54f948654 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:33fb81ef67d1b18100de36153306a5e0 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:33fb81ef67d1b18100de36153306a5e0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:33fb81ef67d1b18100de36153306a5e0 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
            _:33fb81ef67d1b18100de36153306a5e0 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:3c0537e63d84ed27c3141bd9c9280914 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:3c0537e63d84ed27c3141bd9c9280914 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:3c0537e63d84ed27c3141bd9c9280914 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
            _:3c0537e63d84ed27c3141bd9c9280914 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:4b8b60fabbdc5137fc432ea73999f5d0 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:4b8b60fabbdc5137fc432ea73999f5d0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:4b8b60fabbdc5137fc432ea73999f5d0 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#temporalAvailability> .
            _:4b8b60fabbdc5137fc432ea73999f5d0 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:51133baa05f7fbd8069c35d942b48cf6 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:51133baa05f7fbd8069c35d942b48cf6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:51133baa05f7fbd8069c35d942b48cf6 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeMachineInterpretable> .
            _:51133baa05f7fbd8069c35d942b48cf6 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:533bec937eb36ad08bbada485f226ad4 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:533bec937eb36ad08bbada485f226ad4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:533bec937eb36ad08bbada485f226ad4 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#rightsAvailability> .
            _:533bec937eb36ad08bbada485f226ad4 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:546152776a33fae537ee74cb82b2704f <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:546152776a33fae537ee74cb82b2704f <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:546152776a33fae537ee74cb82b2704f <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#knownLicense> .
            _:546152776a33fae537ee74cb82b2704f <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:5967523cf8203c6de1527a2df417f8e7 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:5967523cf8203c6de1527a2df417f8e7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:5967523cf8203c6de1527a2df417f8e7 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsAvailability> .
            _:5967523cf8203c6de1527a2df417f8e7 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:655a2d603373caf2cfea3e26fde4697c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityAnnotation> .
            _:655a2d603373caf2cfea3e26fde4697c <http://www.w3.org/ns/oa#hasBody> <https://data.norge.no/vocabulary/dcatno-mqa#zeroStars> .
            _:655a2d603373caf2cfea3e26fde4697c <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#classifying> .
            _:655a2d603373caf2cfea3e26fde4697c <http://www.w3.org/ns/prov#wasDerivedFrom> _:bd8af50ca3ed16c275c260955b7ef682 .
            _:69ef28f5acbde9c94f21a288c041715b <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:69ef28f5acbde9c94f21a288c041715b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:69ef28f5acbde9c94f21a288c041715b <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability> .
            _:69ef28f5acbde9c94f21a288c041715b <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:77c5880861e68ceccd97f43faf62df0d <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:77c5880861e68ceccd97f43faf62df0d <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:77c5880861e68ceccd97f43faf62df0d <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsVocabularyAlignment> .
            _:77c5880861e68ceccd97f43faf62df0d <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:8391f99de1a1465114fe249cb22fe350 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:8391f99de1a1465114fe249cb22fe350 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:8391f99de1a1465114fe249cb22fe350 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#publisherAvailability> .
            _:8391f99de1a1465114fe249cb22fe350 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:8e39b6cb3cb549181bf3c325f50e5dea <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:8e39b6cb3cb549181bf3c325f50e5dea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:8e39b6cb3cb549181bf3c325f50e5dea <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
            _:8e39b6cb3cb549181bf3c325f50e5dea <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dcat#Distribution> .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:1c1218bfecd7df884f256cbdcff24c8d .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:232150fee40e4bcdc92b65b54f948654 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:33fb81ef67d1b18100de36153306a5e0 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:3c0537e63d84ed27c3141bd9c9280914 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:51133baa05f7fbd8069c35d942b48cf6 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:533bec937eb36ad08bbada485f226ad4 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:546152776a33fae537ee74cb82b2704f .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:a94df11b4e8482bc8cad2e511a013ca7 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:afb21d9ddf0cf29d2495837bb9cd90cf .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:bd8af50ca3ed16c275c260955b7ef682 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d36717340510b75318124cfd4c0e1250 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:d9e5635c3a14b816acce2748167d06a3 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:eb5c20ff509c1cf84ed7ab1d43ce7bd6 .
            _:a1ebdafb6670f791640c7b3facf64b55 <http://www.w3.org/ns/dqv#hasQualityMeasurement> _:f75a6ada9a80cb2ad9837979ff083e88 .
            _:a94df11b4e8482bc8cad2e511a013ca7 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:a94df11b4e8482bc8cad2e511a013ca7 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:a94df11b4e8482bc8cad2e511a013ca7 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
            _:a94df11b4e8482bc8cad2e511a013ca7 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:afb21d9ddf0cf29d2495837bb9cd90cf <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:afb21d9ddf0cf29d2495837bb9cd90cf <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:afb21d9ddf0cf29d2495837bb9cd90cf <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars> .
            _:afb21d9ddf0cf29d2495837bb9cd90cf <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:bd8af50ca3ed16c275c260955b7ef682 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:bd8af50ca3ed16c275c260955b7ef682 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:bd8af50ca3ed16c275c260955b7ef682 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#openLicense> .
            _:bd8af50ca3ed16c275c260955b7ef682 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:ca0550f63c24e83c345fb26ff70fc868 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:ca0550f63c24e83c345fb26ff70fc868 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:ca0550f63c24e83c345fb26ff70fc868 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#spatialAvailability> .
            _:ca0550f63c24e83c345fb26ff70fc868 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:d24a3a2898694457ef5cd197ac98a446 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d24a3a2898694457ef5cd197ac98a446 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d24a3a2898694457ef5cd197ac98a446 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#contactPointAvailability> .
            _:d24a3a2898694457ef5cd197ac98a446 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:d36717340510b75318124cfd4c0e1250 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d36717340510b75318124cfd4c0e1250 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d36717340510b75318124cfd4c0e1250 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
            _:d36717340510b75318124cfd4c0e1250 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:d62ad04a6bb514efb0cc1673dd59221c <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d62ad04a6bb514efb0cc1673dd59221c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d62ad04a6bb514efb0cc1673dd59221c <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#categoryAvailability> .
            _:d62ad04a6bb514efb0cc1673dd59221c <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:d9e5635c3a14b816acce2748167d06a3 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d9e5635c3a14b816acce2748167d06a3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d9e5635c3a14b816acce2748167d06a3 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeNonProprietary> .
            _:d9e5635c3a14b816acce2748167d06a3 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:e7a8b5f28be58d559c99ac8f33d98c2c <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:e7a8b5f28be58d559c99ac8f33d98c2c <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:e7a8b5f28be58d559c99ac8f33d98c2c <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
            _:e7a8b5f28be58d559c99ac8f33d98c2c <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:eb5c20ff509c1cf84ed7ab1d43ce7bd6 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:eb5c20ff509c1cf84ed7ab1d43ce7bd6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:eb5c20ff509c1cf84ed7ab1d43ce7bd6 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeVocabularyAlignment> .
            _:eb5c20ff509c1cf84ed7ab1d43ce7bd6 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 .
            _:f75a6ada9a80cb2ad9837979ff083e88 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:f75a6ada9a80cb2ad9837979ff083e88 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:f75a6ada9a80cb2ad9837979ff083e88 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#byteSizeAvailability> .
            _:f75a6ada9a80cb2ad9837979ff083e88 <http://www.w3.org/ns/dqv#computedOn> _:a1ebdafb6670f791640c7b3facf64b55 ."#,
        )).unwrap();

        assert!(mqa_event.is_ok());

        let mqa_event_raw = mqa_event.unwrap();
        let store_actual = parse_turtle(mqa_event_raw.graph).unwrap();
        assert_eq!(
            store_expected
                .quads_for_pattern(None, None, None, None)
                .count(),
            store_actual
                .quads_for_pattern(None, None, None, None)
                .count()
        );

        match rdf::get_dataset_node(&store_actual) {
            Some(dataset_node) => {
                assert_eq!(
                    1,
                    store_actual
                        .quads_for_pattern(
                            Some(dataset_node.as_ref().into()),
                            Some(dqv::HAS_QUALITY_ANNOTATION),
                            None,
                            None
                        )
                        .count()
                );

                assert_eq!(
                    10,
                    store_actual
                        .quads_for_pattern(
                            Some(dataset_node.as_ref().into()),
                            Some(dqv::HAS_QUALITY_MEASUREMENT),
                            None,
                            None
                        )
                        .count()
                );

                let dist = list_distributions(dataset_node.as_ref(), &store_actual)
                    .next()
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    10,
                    store_actual
                        .quads_for_pattern(
                            Some(dist.subject.as_ref().into()),
                            Some(dqv::HAS_QUALITY_MEASUREMENT),
                            None,
                            None
                        )
                        .count()
                );
            }
            _ => assert!(false, "No dataset found"),
        }
    }
}
