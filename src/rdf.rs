use oxigraph::io::{RdfFormat, RdfParser};
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::*;
use oxigraph::store::{QuadIter, SerializerError, StorageError, Store};

use crate::error::Error;
use crate::vocab::{dcat, dcat_mqa, dcterms, dqv, prov};

/// Parse Turtle RDF and load into store.
pub fn parse_turtle(store: &Store, turtle: String) -> Result<(), Error> {
    store.load_from_reader(
        RdfParser::from_format(RdfFormat::Turtle)
            .without_named_graphs()
            .with_default_graph(GraphNameRef::DefaultGraph),
        turtle.to_string().as_bytes().as_ref()
    )?;
    Ok(())
}

/// Retrieve datasets
pub fn list_datasets(store: &Store) -> QuadIter {
    store.quads_for_pattern(
        None,
        Some(rdf::TYPE),
        Some(dcat::DATASET_CLASS.into()),
        None,
    )
}

/// Retrieve distributions of a dataset
pub fn list_distributions(dataset: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(dataset.into()),
        Some(dcat::DISTRIBUTION.into()),
        None,
        None,
    )
}

/// Retrieve distribution formats
pub fn list_formats(distribution: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcterms::FORMAT.into()),
        None,
        None,
    )
}

/// Retrieve distribution media-types
pub fn list_media_types(distribution: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcat::MEDIA_TYPE.into()),
        None,
        None,
    )
}

/// Retrieve license
pub fn list_licenses(distribution: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcterms::LICENSE.into()),
        None,
        None,
    )
}

/// Retrieve access rights
pub fn list_access_rights(dataset: NamedNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(dataset.into()),
        Some(dcterms::ACCESS_RIGHTS.into()),
        None,
        None,
    )
}

/// Retrieve dataset namednode
pub fn get_dataset_node(store: &Store) -> Option<NamedNode> {
    list_datasets(&store).next().and_then(|d| match d {
        Ok(Quad {
            subject: Subject::NamedNode(n),
            ..
        }) => Some(n),
        _ => None,
    })
}

/// Extract assessment of node.
pub fn node_assessment(store: &Store, node: NamedNodeRef) -> Result<NamedNode, Error> {
    store
        .quads_for_pattern(
            Some(node.into()),
            Some(dcat_mqa::HAS_ASSESSMENT.into()),
            None,
            None,
        )
        .next()
        .ok_or(Error::from(format!(
            "assessment not found for node '{}'",
            node,
        )))?
        .map(|d| match d {
            Quad {
                object: Term::NamedNode(n),
                ..
            } => Ok(n),
            _ => Err(format!(
                "assessment of node '{}' is not a named node: '{}'",
                node, d.object
            )
            .into()),
        })?
}

pub fn has_property(subject: SubjectRef, property: NamedNodeRef, store: &Store) -> bool {
    store
        .quads_for_pattern(Some(subject), Some(property), None, None)
        .count()
        > 0
}

pub fn add_property(
    subject: SubjectRef,
    property: NamedNodeRef,
    object: TermRef,
    store: &Store,
) -> Result<(), StorageError> {
    store.insert(Quad::new(subject, property, object, GraphName::DefaultGraph).as_ref())?;
    Ok(())
}

pub fn add_five_star_annotation(store: &Store) -> Result<BlankNode, StorageError> {
    let five_star_annotation_node = BlankNode::default();
    store.insert(
        Quad::new(
            five_star_annotation_node.as_ref(),
            rdf::TYPE,
            dqv::QUALITY_ANNOTATION_CLASS,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;

    Ok(five_star_annotation_node)
}

pub fn get_five_star_annotation(store: &Store) -> Option<BlankNode> {
    store
        .quads_for_pattern(
            None,
            Some(rdf::TYPE),
            Some(dqv::QUALITY_ANNOTATION_CLASS.into()),
            None,
        )
        .next()
        .and_then(|r| match r {
            Ok(Quad {
                subject: Subject::BlankNode(n),
                ..
            }) => Some(n),
            _ => None,
        })
}

pub fn add_derived_from(
    quality_annotation: NamedOrBlankNodeRef,
    derived_from: NamedOrBlankNodeRef,
    store: &Store,
) -> Result<(), StorageError> {
    store.insert(
        Quad::new(
            quality_annotation,
            prov::WAS_DERIVED_FROM,
            derived_from,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    Ok(())
}

/// Insert dataset assessment into store
pub fn insert_dataset_assessment(
    dataset_assessment: NamedNodeRef,
    dataset: NamedNodeRef,
    store: &Store,
) -> Result<(), Error> {
    store.insert(&Quad::new(
        dataset_assessment.clone(),
        rdf::TYPE,
        dcat_mqa::DATASET_ASSESSMENT_CLASS,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        dataset_assessment.clone(),
        dcat_mqa::ASSESSMENT_OF,
        dataset,
        GraphName::DefaultGraph,
    ))?;

    Ok(())
}

/// Insert distribution assessment into store
pub fn insert_distribution_assessment(
    dataset_assessment: NamedNodeRef,
    distribution_assessment: NamedNodeRef,
    distribution: NamedNodeRef,
    store: &Store,
) -> Result<(), Error> {
    store.insert(&Quad::new(
        distribution_assessment,
        rdf::TYPE,
        dcat_mqa::DISTRIBUTION_ASSESSMENT_CLASS,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        distribution_assessment.clone(),
        dcat_mqa::ASSESSMENT_OF,
        distribution,
        GraphName::DefaultGraph,
    ))?;
    store.insert(&Quad::new(
        dataset_assessment,
        dcat_mqa::HAS_DISTRIBUTION_ASSESSMENT,
        distribution_assessment,
        GraphName::DefaultGraph,
    ))?;

    Ok(())
}

/// Add quality measurement to metric store
pub fn add_quality_measurement(
    metric: NamedNodeRef,
    target: NamedNodeRef,
    computed_on: NamedNodeRef,
    value: bool,
    store: &Store,
) -> Result<BlankNode, StorageError> {
    let measurement = BlankNode::default();
    let value_term = Term::Literal(Literal::new_typed_literal(
        format!("{}", value),
        xsd::BOOLEAN,
    ));

    store.insert(
        Quad::new(
            measurement.as_ref(),
            rdf::TYPE,
            dqv::QUALITY_MEASUREMENT_CLASS,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            measurement.as_ref(),
            dqv::IS_MEASUREMENT_OF,
            metric,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            measurement.as_ref(),
            dqv::COMPUTED_ON,
            computed_on,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            measurement.as_ref(),
            dqv::VALUE,
            value_term,
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;
    store.insert(
        Quad::new(
            target,
            dcat_mqa::CONTAINS_QUALITY_MEASUREMENT,
            measurement.as_ref(),
            GraphName::DefaultGraph,
        )
        .as_ref(),
    )?;

    Ok(measurement)
}

/// Dump graph as turtle string
pub fn dump_graph_as_turtle(store: &Store) -> Result<Vec<u8>, SerializerError> {
    let mut buffer = Vec::new();
    store.dump_graph_to_writer(GraphNameRef::DefaultGraph, RdfFormat::Turtle, &mut buffer)?;
    Ok(buffer)
}

/// Check if format is RDF
pub fn is_rdf_format(format: &str) -> bool {
    match format.to_lowercase().as_str() {
        "rdf" | "turtle" | "ntriples" | "n3" | "nq" | "json-ld" | "jsonld" => true,
        _ => false,
    }
}
