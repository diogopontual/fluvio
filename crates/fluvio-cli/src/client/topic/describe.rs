//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!

use std::sync::Arc;

use display::TopicMetadata;
use fluvio::consumer::ConsumerOffset;
use fluvio_sc_schema::objects::{ListRequest, Metadata};
use fluvio_sc_schema::partition::PartitionSpec;
use tracing::debug;
use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::topic::TopicSpec;

use crate::common::output::Terminal;
use crate::common::OutputFormat;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, Parser)]
pub struct DescribeTopicsOpt {
    /// The name of the Topic to describe
    #[arg(value_name = "name")]
    topic: String,

    #[clap(flatten)]
    output: OutputFormat,
}

impl DescribeTopicsOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let topic = self.topic;
        let output_type = self.output.format;
        debug!("describe topic: {}, {:?}", topic, output_type);

        let admin = fluvio.admin().await;
        let topics = admin.list::<TopicSpec, _>(vec![topic.clone()]).await?;

        let consumers: Vec<ConsumerOffset> = fluvio.consumer_offsets().await?;

        let partitions: Vec<Metadata<PartitionSpec>> = admin
            .list_with_config::<PartitionSpec, String>(ListRequest::default().system(false))
            .await?;

        // let topic_list: Vec<TopicMetadata> = Vec::new();

        let topic_list = topics
            .into_iter()
            .map(|m| {
                let consumers: Vec<ConsumerOffset> = consumers
                    .iter()
                    .filter(|consumer| consumer.topic == topic)
                    .map(|el| el.clone())
                    .collect();
                let partitions: Vec<Metadata<PartitionSpec>> = partitions
                    .iter()
                    .filter(|partition| partition.name == topic)
                    .map(|el| el.clone())
                    .collect();
                TopicMetadata::new(m, partitions, consumers)
            })
            .collect();

        display::describe_topics(topic_list, output_type, out).await?;
        Ok(())
    }
}

mod display {

    use fluvio::{consumer::ConsumerOffset, metadata::topic::ReplicaSpec};
    use comfy_table::Row;
    use fluvio_sc_schema::partition::PartitionSpec;
    use humantime::format_duration;
    use serde::Serialize;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::topic::TopicSpec;

    use crate::common::output::{
        OutputType, OutputError, DescribeObjectHandler, KeyValOutputHandler, TableOutputHandler,
        Terminal,
    };

    #[allow(clippy::redundant_closure)]
    // Connect to Kafka Controller and query server for topic
    pub async fn describe_topics<O>(
        topic_list: Vec<TopicMetadata>,
        output_type: OutputType,
        out: std::sync::Arc<O>,
    ) -> Result<(), OutputError>
    where
        O: Terminal,
    {
        out.describe_objects(&topic_list, output_type)
    }

    #[derive(Serialize, Clone)]
    pub struct TopicMetadata {
        topic_spec: Metadata<TopicSpec>,
        partitions: Vec<Metadata<PartitionSpec>>,
        consumers: Vec<ConsumerOffset>,
    }

    impl TopicMetadata {
        pub fn new(
            topic_spec: Metadata<TopicSpec>,
            partitions: Vec<Metadata<PartitionSpec>>,
            consumers: Vec<ConsumerOffset>,
        ) -> Self {
            Self {
                topic_spec,
                partitions,
                consumers,
            }
        }
    }

    impl DescribeObjectHandler for TopicMetadata {
        fn label() -> &'static str {
            "topic"
        }

        fn label_plural() -> &'static str {
            "topics"
        }

        fn is_ok(&self) -> bool {
            true
        }

        fn is_error(&self) -> bool {
            false
        }

        fn validate(&self) -> Result<(), OutputError> {
            Ok(())
        }
    }

    impl TableOutputHandler for TopicMetadata {
        fn header(&self) -> Row {
            Row::new()
        }

        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            vec![]
        }
    }

    impl KeyValOutputHandler for TopicMetadata {
        /// key value hash map implementation
        fn key_values(&self) -> Vec<(String, Option<String>)> {
            let mut key_values = Vec::new();
            let spec = &self.topic_spec.spec;
            let status = &self.topic_spec.status;

            key_values.push(("Name".to_owned(), Some(self.topic_spec.name.clone())));
            key_values.push(("Type".to_owned(), Some(spec.type_label().to_string())));
            match spec.replicas() {
                ReplicaSpec::Computed(param) => {
                    key_values.push((
                        "Partition Count".to_owned(),
                        Some(param.partitions.to_string()),
                    ));
                    key_values.push((
                        "Replication Factor".to_owned(),
                        Some(param.replication_factor.to_string()),
                    ));
                    key_values.push((
                        "Ignore Rack Assignment".to_owned(),
                        Some(param.ignore_rack_assignment.to_string()),
                    ));
                }
                ReplicaSpec::Assigned(_partitions) => {
                    /*
                    key_values.push((
                        "Assigned Partitions".to_owned(),
                        Some(partitions.maps.clone()),
                    ));
                    */
                }
                ReplicaSpec::Mirror(_config) => {}
            }

            if let Some(dedup) = spec.get_deduplication() {
                key_values.push((
                    "Deduplication Filter".to_owned(),
                    Some(dedup.filter.transform.uses.clone()),
                ));
                key_values.push((
                    "Deduplication Count Bound".to_owned(),
                    Some(dedup.bounds.count)
                        .filter(|c| *c != 0)
                        .as_ref()
                        .map(ToString::to_string),
                ));
                key_values.push((
                    "Deduplication Age Bound".to_owned(),
                    dedup.bounds.age.map(|a| format_duration(a).to_string()),
                ));
            };

            key_values.push((
                "Status".to_owned(),
                Some(status.resolution.resolution_label().to_string()),
            ));
            key_values.push(("Reason".to_owned(), Some(status.reason.clone())));

            key_values.push(("-----------------".to_owned(), None));

            key_values
        }
    }
}
