//!
//! # Describe Topic CLI
//!
//! CLI to describe Topics and their corresponding Partitions
//!

use std::sync::Arc;

use display::{PartitionDetails, TopicMetadata};
use fluvio::consumer::{ConsumerConfigExtBuilder, ConsumerOffset};
use fluvio_future::io::StreamExt;
use fluvio_sc_schema::objects::{ListRequest, Metadata};
use fluvio_sc_schema::partition::{PartitionSpec, PartitionSpecUtils};
use tracing::debug;
use clap::Parser;
use anyhow::Result;
use futures_util::future::join_all;
use fluvio::{Fluvio, Offset};
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
        let consumers: Vec<ConsumerOffset> = fluvio.consumer_offsets().await.unwrap();
        let partitions: Vec<Metadata<PartitionSpec>> = admin
            .list_with_config::<PartitionSpec, String>(ListRequest::default().system(false))
            .await
            .unwrap();

        let futures: Vec<_> = topics
            .into_iter()
            .map(|m| async {
                let mut topic_partitions: Vec<PartitionDetails> = Vec::new();
                //Iterating over paritions to create the pastitions set
                for partition in &partitions {
                    let (partition_topic, partition_idx) =
                        PartitionSpecUtils::split_name(&partition.name);
                    if partition_topic != topic {
                        continue;
                    }
                    let consumers: Vec<String> = consumers
                        .iter()
                        .filter(|consumer| consumer.topic == topic)
                        .map(|el| el.consumer_id.clone())
                        .collect();
                    //Loading last-produced
                    let mut builder = ConsumerConfigExtBuilder::default();
                    let mut stream = fluvio
                        .consumer_with_config(
                            builder
                                .topic(topic.to_string())
                                .partition(partition_idx)
                                .offset_strategy(fluvio::consumer::OffsetManagementStrategy::None)
                                .offset_start(Offset::from_end(1))
                                .build()
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    let n: std::result::Result<fluvio::consumer::Record, fluvio_protocol::link::ErrorCode> = stream.next().await.unwrap();
                    let last_produced = n.unwrap().timestamp();  
                    let last_produced_u64 = last_produced.max(0) as u64;              
                    // admin.create_with_config(config, spec)
                    topic_partitions.push(PartitionDetails::new(
                        partition_idx,
                        partition.status.leader.leo,
                        last_produced_u64,
                        consumers,
                    ));
                }

                TopicMetadata::new(m, topic_partitions)
            })
            .collect();

        let topic_list = join_all(futures).await;
        display::describe_topics(topic_list, output_type, out).await?;
        Ok(())
    }
}

mod display {

    use std::time::{Duration, SystemTime};

    use fluvio::metadata::topic::ReplicaSpec;
    use comfy_table::{Cell, Row};
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
        partitions: Vec<PartitionDetails>,
    }

    #[derive(Serialize, Clone, Debug)]
    pub struct PartitionDetails {
        partition: u32,
        last_offset: i64,
        last_produced: u64,
        consumers: Vec<String>,
    }

    impl PartitionDetails {
        pub fn new(
            partition: u32,
            last_offset: i64,
            last_produced: u64,
            consumers: Vec<String>,
        ) -> Self {
            Self {
                partition,
                last_offset,
                last_produced,
                consumers,
            }
        }
    }

    impl TopicMetadata {
        pub fn new(topic_spec: Metadata<TopicSpec>, partitions: Vec<PartitionDetails>) -> Self {
            Self {
                topic_spec,
                partitions,
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
            Row::from(["PARTITION", "LAST-OFFSET", "LAST-PRODUCED", "CONSUMERS"])
        }

        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let partitions = self.partitions.clone();
            // list.sort();
            partitions
                .into_iter()
                .map(|partition| {
                    println!("{:?} : {:?}", now, partition.last_produced);
                    let last_produced = humantime::Duration::from(Duration::from_millis(
                        (now as u64) - partition.last_produced,
                    ));
                    Row::from([
                        Cell::new(partition.partition),
                        Cell::new(partition.last_offset),
                        Cell::new(last_produced),
                        Cell::new(partition.consumers.join(", ")),
                    ])
                })
                .collect()
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
