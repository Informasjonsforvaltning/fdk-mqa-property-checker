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
        list_distributions, list_formats, list_media_types, node_assessment, parse_turtle,
    },
    reference_data::{valid_file_type, valid_media_type},
    vocab::{dcat, dcat_mqa, dcterms, oa},
};

pub fn parse_rdf_graph_and_calculate_metrics(
    input_store: &Store,
    output_store: &Store,
    graph: String,
) -> Result<String, Error> {
    input_store.clear()?;
    output_store.clear()?;
    parse_turtle(input_store, graph)?;
    let dataset_node = get_dataset_node(input_store).ok_or("Dataset node not found in graph")?;
    calculate_metrics(dataset_node.as_ref(), input_store, output_store)?;
    let bytes = dump_graph_as_turtle(output_store)?;
    let turtle = std::str::from_utf8(bytes.as_slice())
        .map_err(|e| format!("Failed converting graph to string: {}", e))?;
    Ok(turtle.to_string())
}

fn calculate_metrics(
    dataset_node: NamedNodeRef,
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

    // TODO Verify if valid license uri
    add_quality_measurement(
        dcat_mqa::ACCESS_RIGHTS_VOCABULARY_ALIGNMENT,
        dataset_assessment.as_ref(),
        dataset_node.into(),
        false,
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
        )?;
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

fn calculate_distribution_metrics(
    dist_assessment_node: NamedNodeRef,
    dist_node: NamedNodeRef,
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
        dist_assessment_node,
        dist_node.into(),
        is_format_aligned || is_media_type_aligned,
        &metrics_store,
    )?;

    if has_license_property {
        // TODO
        add_quality_measurement(
            dcat_mqa::KNOWN_LICENSE,
            dist_assessment_node,
            dist_node.into(),
            false,
            &metrics_store,
        )?;

        // TODO
        five_star_open_license_derived_from = Some(add_quality_measurement(
            dcat_mqa::OPEN_LICENSE,
            dist_assessment_node,
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
    use crate::vocab::dcat_mqa;

    use super::*;
    use oxigraph::model::{vocab, Subject};
    use std::env;

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

        env::set_var(
            "REFERENCE_DATA_BASE_URL",
            format!("http://{}", server.address()),
        );

        let mqa_graph = parse_rdf_graph_and_calculate_metrics(&mut Store::new().unwrap(), &mut Store::new().unwrap(), r#"
            @prefix adms: <http://www.w3.org/ns/adms#> . 
            @prefix cpsv: <http://purl.org/vocab/cpsv#> . 
            @prefix cpsvno: <https://data.norge.no/vocabulary/cpsvno#> . 
            @prefix dcat: <http://www.w3.org/ns/dcat#> . 
            @prefix dcatnomqa: <https://data.norge.no/vocabulary/dcatno-mqa#> . 
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
            
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> rdf:type dcat:Distribution ; dct:description "Norsk bistand i tall etter partner"@nb ; 
                dcatnomqa:hasAssessment <http://dist.foo.assessment.no> ;
                dct:format <https://www.iana.org/assignments/media-types/application/vnd.openxmlformats-officedocument.spreadsheetml.sheet> , 
                        <https://www.iana.org/assignments/media-types/text/csv> ; 
                dct:license <http://data.norge.no/nlod/no/2.0> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dcat:accessURL <https://resultater.norad.no/partner/> .
                
            <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> rdf:type dcat:Dataset ; 
                dcatnomqa:hasAssessment <http://dataset.assessment.no> ;
                dct:accessRights <http://publications.europa.eu/resource/authority/access-right/PUBLIC> ; 
                dct:description "Visning over all norsk offentlig bistand fra 1960 til siste kalenderår sortert etter partnerorganisasjoner."@nb ; 
                dct:identifier "https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572" ; 
                dct:language <http://publications.europa.eu/resource/authority/language/NOR> , <http://publications.europa.eu/resource/authority/language/ENG> ; 
                dct:provenance <http://data.brreg.no/datakatalog/provinens/nasjonal> ; 
                dct:publisher <https://organization-catalogue.fellesdatakatalog.digdir.no/organizations/971277882> ; 
                dct:title "Bistandsresultater - bistand etter partner"@nb ; 
                dct:type "Data" ; 
                dcat:contactPoint [ rdf:type vcard:Organization ; vcard:hasEmail <mailto:resultater@norad.no> ] ; 
                dcat:distribution <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> ; 
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
        "#.to_string()).unwrap();

        let store_expected = Store::new().unwrap();
        parse_turtle(&store_expected, String::from(
            r#"<https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityAnnotation> _:a1f6bdfa800f9044fc9e18f5bbfa42e5 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            <http://dataset.assessment.no> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DatasetAssessment> .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#hasDistributionAssessment> <http://dist.foo.assessment.no> .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:680215e3ec0228c896fd801114a2a0e .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:7ae51b6452d773c6c600de5c0abfcb8 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:81050be482bb0da9ea051295ee5b337 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:3d18702ae85cee4e17b0919ece050427 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:4e44066288b45da96c74c3526b8f4780 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:5b68616d5e3f2aeadd4c934031746e46 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:a762b8c94ac171a937c09f254a916e3f .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:aed131fc474541da56e65ce38bd19bb4 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:bd0df7c46a1a49b68b5e0b67bc4975b1 .
            <http://dataset.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:d60f7380c1750c4a0fc22a712e395282 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#assessmentOf> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            <http://dist.foo.assessment.no> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://data.norge.no/vocabulary/dcatno-mqa#DistributionAssessment> .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:59bb90a6bd3974547dd563dad0ff3e2 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:9c62b4d8d36e8c4e70d7ddf05672bb1 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:e8612c0caca4404ff03d09388eb3acf .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:2618f39594a4900893f78e29d841ec77 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:2c7785200ea58d37e0485c381ffc4af5 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:36f67131cd1db53fe6a93b49883f2c40 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:4fbb90d09c2120281a38490b0ceb11ef .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:6df030a4d515856d5f615c94ea3a4e06 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:81ed38c70c900bb0456d35f0c1b94056 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:88f83ad9cfc3a3ea547465f01018f437 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:8c8aa449ce09b41fdf966b4f934a1e47 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:93795091984d9326e96656db59825dc1 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:ab31464750546984b59f7f599247f666 .
            <http://dist.foo.assessment.no> <https://data.norge.no/vocabulary/dcatno-mqa#containsQualityMeasurement> _:da6e2e0bdb700a746368ded59c8920f0 .
            _:59bb90a6bd3974547dd563dad0ff3e2 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:59bb90a6bd3974547dd563dad0ff3e2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:59bb90a6bd3974547dd563dad0ff3e2 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#byteSizeAvailability> .
            _:59bb90a6bd3974547dd563dad0ff3e2 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:680215e3ec0228c896fd801114a2a0e <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:680215e3ec0228c896fd801114a2a0e <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:680215e3ec0228c896fd801114a2a0e <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#keywordAvailability> .
            _:680215e3ec0228c896fd801114a2a0e <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:7ae51b6452d773c6c600de5c0abfcb8 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:7ae51b6452d773c6c600de5c0abfcb8 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:7ae51b6452d773c6c600de5c0abfcb8 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsAvailability> .
            _:7ae51b6452d773c6c600de5c0abfcb8 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:81050be482bb0da9ea051295ee5b337 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:81050be482bb0da9ea051295ee5b337 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:81050be482bb0da9ea051295ee5b337 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
            _:81050be482bb0da9ea051295ee5b337 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:9c62b4d8d36e8c4e70d7ddf05672bb1 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:9c62b4d8d36e8c4e70d7ddf05672bb1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:9c62b4d8d36e8c4e70d7ddf05672bb1 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#downloadUrlAvailability> .
            _:9c62b4d8d36e8c4e70d7ddf05672bb1 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:e8612c0caca4404ff03d09388eb3acf <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:e8612c0caca4404ff03d09388eb3acf <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:e8612c0caca4404ff03d09388eb3acf <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#rightsAvailability> .
            _:e8612c0caca4404ff03d09388eb3acf <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:2618f39594a4900893f78e29d841ec77 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:2618f39594a4900893f78e29d841ec77 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:2618f39594a4900893f78e29d841ec77 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
            _:2618f39594a4900893f78e29d841ec77 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:2c7785200ea58d37e0485c381ffc4af5 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:2c7785200ea58d37e0485c381ffc4af5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:2c7785200ea58d37e0485c381ffc4af5 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeVocabularyAlignment> .
            _:2c7785200ea58d37e0485c381ffc4af5 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:36f67131cd1db53fe6a93b49883f2c40 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:36f67131cd1db53fe6a93b49883f2c40 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:36f67131cd1db53fe6a93b49883f2c40 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeMachineInterpretable> .
            _:36f67131cd1db53fe6a93b49883f2c40 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:3d18702ae85cee4e17b0919ece050427 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:3d18702ae85cee4e17b0919ece050427 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:3d18702ae85cee4e17b0919ece050427 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#accessRightsVocabularyAlignment> .
            _:3d18702ae85cee4e17b0919ece050427 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:4e44066288b45da96c74c3526b8f4780 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:4e44066288b45da96c74c3526b8f4780 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:4e44066288b45da96c74c3526b8f4780 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#categoryAvailability> .
            _:4e44066288b45da96c74c3526b8f4780 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:4fbb90d09c2120281a38490b0ceb11ef <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:4fbb90d09c2120281a38490b0ceb11ef <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:4fbb90d09c2120281a38490b0ceb11ef <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#licenseAvailability> .
            _:4fbb90d09c2120281a38490b0ceb11ef <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:5b68616d5e3f2aeadd4c934031746e46 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:5b68616d5e3f2aeadd4c934031746e46 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:5b68616d5e3f2aeadd4c934031746e46 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateModifiedAvailability> .
            _:5b68616d5e3f2aeadd4c934031746e46 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:6df030a4d515856d5f615c94ea3a4e06 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:6df030a4d515856d5f615c94ea3a4e06 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:6df030a4d515856d5f615c94ea3a4e06 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#mediaTypeAvailability> .
            _:6df030a4d515856d5f615c94ea3a4e06 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:81ed38c70c900bb0456d35f0c1b94056 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:81ed38c70c900bb0456d35f0c1b94056 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:81ed38c70c900bb0456d35f0c1b94056 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#atLeastFourStars> .
            _:81ed38c70c900bb0456d35f0c1b94056 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:88f83ad9cfc3a3ea547465f01018f437 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:88f83ad9cfc3a3ea547465f01018f437 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:88f83ad9cfc3a3ea547465f01018f437 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#knownLicense> .
            _:88f83ad9cfc3a3ea547465f01018f437 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:8c8aa449ce09b41fdf966b4f934a1e47 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:8c8aa449ce09b41fdf966b4f934a1e47 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:8c8aa449ce09b41fdf966b4f934a1e47 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatAvailability> .
            _:8c8aa449ce09b41fdf966b4f934a1e47 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:93795091984d9326e96656db59825dc1 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:93795091984d9326e96656db59825dc1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:93795091984d9326e96656db59825dc1 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#dateIssuedAvailability> .
            _:93795091984d9326e96656db59825dc1 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:a1f6bdfa800f9044fc9e18f5bbfa42e5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityAnnotation> .
            _:a1f6bdfa800f9044fc9e18f5bbfa42e5 <http://www.w3.org/ns/oa#hasBody> <https://data.norge.no/vocabulary/dcatno-mqa#zeroStars> .
            _:a1f6bdfa800f9044fc9e18f5bbfa42e5 <http://www.w3.org/ns/oa#motivatedBy> <http://www.w3.org/ns/oa#classifying> .
            _:a1f6bdfa800f9044fc9e18f5bbfa42e5 <http://www.w3.org/ns/prov#wasDerivedFrom> _:da6e2e0bdb700a746368ded59c8920f0 .
            _:a762b8c94ac171a937c09f254a916e3f <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:a762b8c94ac171a937c09f254a916e3f <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:a762b8c94ac171a937c09f254a916e3f <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#temporalAvailability> .
            _:a762b8c94ac171a937c09f254a916e3f <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:ab31464750546984b59f7f599247f666 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:ab31464750546984b59f7f599247f666 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:ab31464750546984b59f7f599247f666 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#formatMediaTypeNonProprietary> .
            _:ab31464750546984b59f7f599247f666 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> .
            _:aed131fc474541da56e65ce38bd19bb4 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:aed131fc474541da56e65ce38bd19bb4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:aed131fc474541da56e65ce38bd19bb4 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#publisherAvailability> .
            _:aed131fc474541da56e65ce38bd19bb4 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:bd0df7c46a1a49b68b5e0b67bc4975b1 <http://www.w3.org/ns/dqv#value> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:bd0df7c46a1a49b68b5e0b67bc4975b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:bd0df7c46a1a49b68b5e0b67bc4975b1 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#contactPointAvailability> .
            _:bd0df7c46a1a49b68b5e0b67bc4975b1 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:d60f7380c1750c4a0fc22a712e395282 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:d60f7380c1750c4a0fc22a712e395282 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:d60f7380c1750c4a0fc22a712e395282 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#spatialAvailability> .
            _:d60f7380c1750c4a0fc22a712e395282 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572> .
            _:da6e2e0bdb700a746368ded59c8920f0 <http://www.w3.org/ns/dqv#value> "false"^^<http://www.w3.org/2001/XMLSchema#boolean> .
            _:da6e2e0bdb700a746368ded59c8920f0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/dqv#QualityMeasurement> .
            _:da6e2e0bdb700a746368ded59c8920f0 <http://www.w3.org/ns/dqv#isMeasurementOf> <https://data.norge.no/vocabulary/dcatno-mqa#openLicense> .
            _:da6e2e0bdb700a746368ded59c8920f0 <http://www.w3.org/ns/dqv#computedOn> <https://registrering.fellesdatakatalog.digdir.no/catalogs/971277882/datasets/29a2bf37-5867-4c90-bc74-5a8c4e118572/.well-known/skolem/1> ."#,
        )).unwrap();

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
        } else {
            panic!("Distribution assessment is not a named node")
        };
    }
}
