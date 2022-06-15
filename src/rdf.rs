use log::info;

use oxigraph::io::GraphFormat;
use oxigraph::model::vocab::{rdf, xsd};
use oxigraph::model::*;
use oxigraph::store::{QuadIter, SerializerError, StorageError, Store};

use crate::error::Error;
use crate::vocab::{dcat, dcterms, dqv, prov};

/// Parse Turtle RDF and load into store
pub fn parse_turtle(turtle: String) -> Result<Store, Error> {
    info!("Loading turtle graph");

    let store = Store::new()?;
    store.load_graph(
        turtle.as_ref(),
        GraphFormat::Turtle,
        GraphNameRef::DefaultGraph,
        None,
    )?;

    Ok(store)
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
pub fn list_formats(distribution: NamedOrBlankNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcterms::FORMAT.into()),
        None,
        None,
    )
}

/// Retrieve distribution media-types
pub fn list_media_types(distribution: NamedOrBlankNodeRef, store: &Store) -> QuadIter {
    store.quads_for_pattern(
        Some(distribution.into()),
        Some(dcat::MEDIA_TYPE.into()),
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

pub fn convert_term_to_named_or_blank_node_ref(term: TermRef) -> Option<NamedOrBlankNodeRef> {
    match term {
        TermRef::NamedNode(node) => Some(NamedOrBlankNodeRef::NamedNode(node)),
        TermRef::BlankNode(node) => Some(NamedOrBlankNodeRef::BlankNode(node)),
        _ => None,
    }
}

/// Create new memory metrics store for supplied dataset
pub fn create_metrics_store(dataset: NamedNodeRef) -> Result<Store, StorageError> {
    let store = Store::new()?;

    // Insert dataset
    store.insert(&Quad::new(
        dataset.clone(),
        rdf::TYPE,
        dcat::DATASET_CLASS,
        GraphName::DefaultGraph,
    ))?;
    Ok(store)
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

/// Add quality measurement to metric store
pub fn add_quality_measurement(
    metric: NamedNodeRef,
    computed_on: NamedOrBlankNodeRef,
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
            computed_on,
            dqv::HAS_QUALITY_MEASUREMENT,
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
    store.dump_graph(&mut buffer, GraphFormat::Turtle, GraphNameRef::DefaultGraph)?;
    Ok(buffer)
}

/// Check if format is RDF
pub fn is_rdf_format(format: &str) -> bool {
    match format.to_lowercase().as_str() {
        "rdf" | "turtle" | "ntriples" | "n3" | "nq" | "json-ld" | "jsonld" => true,
        _ => false,
    }
}
