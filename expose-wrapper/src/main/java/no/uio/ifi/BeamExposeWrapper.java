package no.uio.ifi;

import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Future;

@SuppressWarnings("unchecked")
public class BeamExposeWrapper implements ExperimentAPI, Serializable {
    static long timeLastRecvdTuple = 0;
    static long number_received = 0;
    int batch_size;
    int pktsPublished;
    int interval_wait;
    final int TIMELASTRECEIVEDTHRESHOLD = 1000;  // ms
    boolean useRowtime = true;
    String trace_output_folder;
    List<Map<String, Object>> fetchQueries = new ArrayList<>();
    ArrayList<Map<String, Object>> queries = new ArrayList<>();
    Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
    Map<String, Integer> streamNameToId = new HashMap<>();
    Map<Integer, String> streamIdToName = new HashMap<>();
    Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
    Map<Integer, List<Integer>> streamIdToNodeIds = new HashMap<>();
    Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();
    Map<Integer, Schema> streamIdToSchema = new HashMap<>();
    Map<Integer, Boolean> printDataStream = new HashMap<>();
    Map<Integer, PCollection<Row>> streamIdToKafkaCollection = new HashMap<>();
    static TracingFramework tf = new TracingFramework();
    Thread threadRunningEnvironment;
    static int nodeId;
    Properties props;
    Map<String, Object> map_props;
    Map<Integer, KafkaProducer> nodeIdToKafkaProducer = new HashMap<>();
    Map<Integer, Properties> nodeIdToProperties = new HashMap<>();
    Map<Integer, List<Tuple2<String, byte[]>>> dataset_to_tuples = new HashMap<>();
    PCollectionTuple pCollectionTuple = null;

    Pipeline pipeline;
    PipelineOptions options;

    BeamExposeWrapper() {
        props = new Properties();
        map_props = new HashMap<>();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "Group");
        //props.put("auto.offset.reset", "latest");
        //props.put("client.id", Integer.toString(nodeId));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        //props.put("topic.timestamp.type", "LogAppendTime");
        //props.put("message.timestamp.type", "LogAppendTime");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        for (Object key : props.keySet()) {
            map_props.put((String)key, props.get(key));
        }

        String[] args = new String[]{"--runner=FlinkRunner"};
        //String[] args = new String[]{""};
        options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
        pipeline = Pipeline.create(options);
    }

    public void setNodeId(int nodeId) {
        BeamExposeWrapper.nodeId = nodeId;
        props.put("client.id", Integer.toString(nodeId));
        //producer = new KafkaProducer<>(props);
    }

    public void SetTraceOutputFolder(String f) {this.trace_output_folder = f;}

    @Override
    public String SetTupleBatchSize(int size) {
        batch_size = size;
        return "Success";
    }

    @Override
    public String SetIntervalBetweenTuples(int interval) {
        interval_wait = interval;
        return "Success";
    }

    @Override
    public String SetNidToAddress(Map<Integer, Map<String, Object> > newNodeIdToIpAndPort) {
        //System.out.println("SetNidToAddress: " + newNodeIdToIpAndPort);
        nodeIdToIpAndPort = newNodeIdToIpAndPort;
        Properties properties = new Properties();
        for (Object key : props.keySet()) {
            properties.put(key, props.get(key));
        }

        for (int nodeId : nodeIdToIpAndPort.keySet()) {
            String ip = (String) nodeIdToIpAndPort.get(nodeId).get("ip");
            properties.put("bootstrap.servers", ip+":9092");
            //System.out.println("Setting bootstrap server node ID " + nodeId + ": " + ip + ":9092");
            //properties.put("client.id", Integer.toString(nodeId));
            if (nodeIdToProperties.get(nodeId) == null) {
                System.out.println("SetNidToAddress, nodeId: " + nodeId + ", ip: " + ip);
                nodeIdToProperties.put(nodeId, properties);
                nodeIdToKafkaProducer.put(nodeId, new KafkaProducer<>(properties));
            }
        }
        return "Success";
    }

    @Override
    public String AddNextHop(int streamId, int nodeId) {
        if (!streamIdToNodeIds.containsKey(streamId)) {
            streamIdToNodeIds.put(streamId, new ArrayList<>());
        }

        // TODO: do something here?
        streamIdToNodeIds.get(streamId).add(nodeId);
        return "Success";
    }

    @Override
    public String WriteStreamToCsv(int stream_id, String csvFolder) {
        PCollection<Row> kafka_collection = streamIdToKafkaCollection.get(stream_id);

        String path = null;
        int cnt = 1;
        boolean finished = false;
        while (!finished) {
            path = System.getenv().get("EXPOSE_PATH") + "/" + csvFolder + "/beam/" + cnt;
            Path p = Paths.get(path);
            if (Files.exists(p)) {
                ++cnt;
                continue;
            }
            finished = true;
        }

        PCollection<String> string_source =
                kafka_collection.apply(MapElements.into(TypeDescriptors.strings())
                        .via(Row::toString));
        string_source.apply(Window.<String>into(new GlobalWindows())
                .triggering(Repeatedly
                        .forever(AfterProcessingTime
                                .pastFirstElementInPane()
                                .plusDelayOf(Duration.standardSeconds(30))
                        )).withAllowedLateness(Duration.standardDays(1)).discardingFiredPanes()).apply(TextIO.write().to(path).withWindowedWrites().withNumShards(1));
        return "Success";
    }

    static class KafkaConsumerDoFn extends DoFn<KV<String, byte[]>, Row> {
        Schema schema;
        Row row = null;
        RowCoder rc;

        KafkaConsumerDoFn(Schema schema) {
            this.schema = schema;
            this.rc = RowCoder.of(schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            timeLastRecvdTuple = System.currentTimeMillis();
            ++number_received;
            tf.traceEvent(1);
            try {
                row = CoderUtils.decodeFromByteArray(rc, c.element().getValue());
            } catch (CoderException e) {
                e.printStackTrace();
            }
            c.output(row);
        }
    }

    volatile static int[] cnt = {0};

    // TODO: Fix this method. Not consuming correctly
    private void AddKafkaConsumer(Map<String, Object> s) {
        int stream_id = (int) s.get("stream-id");
        String topic = streamIdToName.get(stream_id) + "-" + BeamExposeWrapper.nodeId;

        /*AdminClient admin = AdminClient.create(props);
        Map<String, String> configs = new HashMap<>();
        configs.put("message.timestamp.type", "LogAppendTime");
        int partitions = 1;
        short replication = 1;
        admin.createTopics(Arrays.asList(new NewTopic("topic", partitions, replication).configs(configs)));*/
        System.out.println("Subscribing to topic " + topic);
        Schema schema = streamIdToSchema.get(stream_id);
        PCollection<Row> row_collection = pipeline.apply(KafkaIO.<String, byte[]>read()
                .withConsumerConfigUpdates(map_props)
                .withBootstrapServers("localhost:9092")
                .withTopic(topic)  // use withTopics(List<String>) to read from multiple topics.
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)

                // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>

                // Rest of the settings are optional :

                // set event times and watermark based on LogAppendTime. To provide a custom
                // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
                //.withLogAppendTime()

                // restrict reader to committed messages on Kafka (see method documentation).
                .withReadCommitted()

                // offset consumed by the pipeline can be committed back.
                .commitOffsetsInFinalize()

                // finally, if you don't need Kafka metadata, you can drop it.
                .withoutMetadata() // PCollection<KV<Long, String>>
        ).apply(
            ParDo.of(new KafkaConsumerDoFn(schema))
        );

        row_collection.setCoder(RowCoder.of(streamIdToSchema.get(stream_id)));
        // This is necessary for joining tuples AND aggregation functions. Without it, joining and agg queries FAIL to be deployed.
        // Improve this window thing.
        row_collection = row_collection.apply("windowing", Window.<Row>into(SlidingWindows.of(Duration.millis(1)))
                .discardingFiredPanes()
        );
        streamIdToKafkaCollection.put(stream_id, row_collection);
    }

    void CastTuplesCorrectTypes(List<Map<String, Object>> tuples, Map<String, Object> schema) {
        List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
        for (Map<String, Object> tuple : tuples) {
            List<Map<String, Object>> attributes = (List<Map<String, Object>>) tuple.get("attributes");
            for (int i = 0; i < tuple_format.size(); i++) {
                Map<String, String> attribute_format = tuple_format.get(i);
                Map<String, Object> attribute = attributes.get(i);
                switch (attribute_format.get("type")) {
                    case "string":
                        attribute.put("value", attribute.get("value").toString());
                        break;
                    case "bool":
                        attribute.put("value", Boolean.valueOf(attribute.get("value").toString()));
                        break;
                    case "int":
                        attribute.put("value", Integer.parseInt(attribute.get("value").toString()));
                        break;
                    case "float":
                        attribute.put("value", Float.parseFloat(attribute.get("value").toString()));
                        break;
                    case "double":
                        attribute.put("value", Double.parseDouble(attribute.get("value").toString()));
                        break;
                    case "long":
                        attribute.put("value", Long.parseLong(attribute.get("value").toString()));
                        break;
                    case "long-timestamp":
                    case "timestamp":
                        attribute.put("value", new DateTime(Long.parseLong(attribute.get("value").toString())));
                        break;
                    default:
                        throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }
        }
    }

    class SerializeTuples implements Runnable {
        int cnt5 = 0;
        int ds_id;
        int min_offset;
        int max_offset;
        int offset;
        List<Map<String, Object>> tuples;
        List<Tuple2<String, byte[]>> serialized_rows = new ArrayList();

        SerializeTuples(List<Map<String, Object>> tuples, int ds_id, int min_offset, int max_offset) {
            this.ds_id = ds_id;
            this.min_offset = min_offset;
            this.offset = min_offset;
            this.max_offset = max_offset;
            this.tuples = tuples;
        }

        public void run() {
            for (Map<String, Object> tuple : this.tuples.subList(min_offset, max_offset + 1)) {
                int stream_id = (Integer)tuple.get("stream-id");
                String stream_name = BeamExposeWrapper.this.streamIdToName.get(stream_id);
                Schema schema = BeamExposeWrapper.this.streamIdToSchema.get(stream_id);
                org.apache.beam.sdk.values.Row.Builder rowBuilder = Row.withSchema(schema);
                for (Map<String, String> attribute : (List<Map<String, String>>) tuple.get("attributes")) {
                    rowBuilder.addValue(attribute.get("value"));
                }

                RowCoder rc = RowCoder.of(schema);
                Row row = rowBuilder.build();
                byte[] serialized_row = null;

                try {
                    serialized_row = CoderUtils.encodeToByteArray(rc, row);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(18);
                }

                this.serialized_rows.add(new Tuple2<>(stream_name, serialized_row));
                if (++this.cnt5 % 10000 == 0) {
                    System.out.println("Serialized tuple " + this.cnt5);
                }
            }
        }
    }

    @Override
    public String SendDsAsStream(Map<String, Object> ds) {
        //System.out.println("Processing dataset");
        int ds_id = (int) ds.get("id");
        List<Tuple2<String, byte[]>> tuples2 = dataset_to_tuples.get(ds_id);
        //List<Map<String, Object>> tuples = datasetIdToTuples.get(ds_id);
        if (tuples2 == null) {
            List<Map<String, Object>> tuples = new ArrayList<>();
            dataset_to_tuples.put(ds_id, new ArrayList<>());
            Map<String, Object> map = GetMapFromYaml(ds);
            List<Map<String, Object>> raw_tuples = (List<Map<String, Object>>) map.get("cepevents");
            Map<Integer, List<Map<String, Object>>> ordered_tuples = new HashMap<>();
            //int i = 0;
            // Add order to tuples and place them in ordered_tuples
            for (Map<String, Object> tuple : raw_tuples) {
                //tuple.put("_order", i++);
                int tuple_stream_id = (int) tuple.get("stream-id");
                if (ordered_tuples.get(tuple_stream_id) == null) {
                    ordered_tuples.put(tuple_stream_id, new ArrayList<>());
                }
                ordered_tuples.get(tuple_stream_id).add(tuple);
            }

            // Fix the type of the tuples in ordered_tuples
            for (int stream_id : ordered_tuples.keySet()) {
                Map<String, Object> schema = allSchemas.get(stream_id);
                CastTuplesCorrectTypes(ordered_tuples.get(stream_id), schema);
            }
            datasetIdToTuples.put(ds_id, raw_tuples);
            tuples = raw_tuples;

            List<Thread> threads = new ArrayList();
            List<BeamExposeWrapper.SerializeTuples> serializers = new ArrayList();
            Runtime runtime = Runtime.getRuntime();
            int numberOfLogicalCores = runtime.availableProcessors();

            System.out.println("Number of tuples: " + tuples.size());
            int min_offset = 0;
            int tuples_per_core = tuples.size() / numberOfLogicalCores * 1 - 1;
            for(int i = 0; i < numberOfLogicalCores; ++i) {
                int max_offset = min_offset + tuples_per_core;
                if (i == numberOfLogicalCores - 1) {
                    max_offset = tuples.size() - 1;
                }

                BeamExposeWrapper.SerializeTuples serializer = new BeamExposeWrapper.SerializeTuples(tuples, ds_id, min_offset, max_offset);
                serializers.add(serializer);
                Thread t = new Thread(serializer);
                t.start();
                threads.add(t);
                min_offset = max_offset + 1;
            }

            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            List<Tuple2<String, byte[]>> serialized_tuples = this.dataset_to_tuples.get(ds_id);

            for(int i = 0; i < serializers.size(); ++i) {
                serialized_tuples.addAll((serializers.get(i)).serialized_rows);
            }

            tuples2 = this.dataset_to_tuples.get(ds_id);
            System.out.println("Finished loading tuples");
        }


        System.out.println("Transmitting " + tuples2.size() + " tuples");
        for (Tuple2<String, byte[]> tuple : tuples2) {
            String stream_name = tuple.f0;
            byte[] serialized_row = tuple.f1;
            for (int otherNodeId : streamIdToNodeIds.getOrDefault(streamNameToId.get(stream_name), new ArrayList<>())) {
                String topicName = stream_name + "-" + otherNodeId;
                if (++tupleCnt % 100000 == 0) {
                    System.out.println( System.nanoTime() + ": Sending tuple " + (++tupleCnt) + " to node " + otherNodeId + " with topic " + topicName);
                }
                Future<RecordMetadata> future = this.nodeIdToKafkaProducer.get(otherNodeId).send(new ProducerRecord<>(topicName, serialized_row));
            }
        }
        return "Success";
    }

    @Override
    public String AddTuples(Map<String, Object> tuple, int quantity) {
        return "Success";
    }

    @Override
    public String AddSchemas(List<Map<String, Object>> schemas) {
        for (Map<String, Object> schema : schemas) {
            int stream_id = (int) schema.get("stream-id");
            final String stream_name = (String) schema.get("name");
            streamIdToName.put(stream_id, stream_name);
            streamNameToId.put(stream_name, stream_id);
            allSchemas.put(stream_id, schema);
            ArrayList<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
            //TypeInformation<?>[] typeInformations = new TypeInformation[tuple_format.size()];
            int pos = -1;
            String rowtime_column = null;
            if (useRowtime && schema.containsKey("rowtime-column")) {
                rowtime_column = ((Map<String, String>)schema.get("rowtime-column")).get("column");
            }

            Schema.Builder b = Schema.builder();
            for (Map<String, String> attribute : tuple_format) {
                pos += 1;
                if (useRowtime && attribute.get("name") != null && attribute.get("name").equals(rowtime_column)) {
                    attribute.put("name", "eventTime");
                }

                String name = attribute.get("name");
                switch (attribute.get("type")) {
                    case "string":
                        b.addStringField(name);
                        break;
                    case "bool":
                        b.addBooleanField(name);
                        break;
                    case "int":
                        b.addInt32Field(name);
                        break;
                    case "float":
                        b.addFloatField(name);
                        break;
                    case "double":
                        b.addDoubleField(name);
                        break;
                    case "long":
                        b.addInt64Field(name);
                        break;
                    case "long-timestamp":
                    case "timestamp":
                        b.addDateTimeField(name);
                        break;
                    default:
                        throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }

            streamIdToSchema.put(stream_id, b.build());
        }
        return "Success";
    }

    static class QueryDoFn extends DoFn<Row, KV<String, byte[]>> {
        String outputStreamName;
        Schema schema;
        RowCoder rc;
        static ByteArrayOutputStream bos = new ByteArrayOutputStream();

        QueryDoFn(String outputStreamName, Schema schema) {
            this.outputStreamName = outputStreamName;
            this.schema = schema;
            this.rc = RowCoder.of(schema);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            byte[] encoded = null;
            try {
                encoded = CoderUtils.encodeToByteArray(rc, c.element());
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(12);
            }
            tf.traceEvent(100);
            c.output(KV.of(outputStreamName, encoded));
        }
    }

    public void DeployQueries() {
        for (Map<String, Object> query : fetchQueries) {
            int outputStreamId = (int) query.get("output-stream-id");
            String outputStreamName = streamIdToName.get(outputStreamId);
            //Map<String, Object> output_schema = this.allSchemas.get(outputStreamId);
            String sql_query = ((Map<String, String>) query.get("sql-query")).get("beam");

            Schema schema = streamIdToSchema.get(outputStreamId);

            PCollection<Row> queryOutput = pCollectionTuple
                    .apply(SqlTransform.query(sql_query).registerUdf("DOLTOEUR", new DolToEur()));

            PCollection<Row> queryOutputToBeFedBack = queryOutput.apply("windowing", Window.<Row>into(SlidingWindows.of(Duration.millis(1)))
                    .discardingFiredPanes()
            );
            queryOutputToBeFedBack.setCoder(RowCoder.of(streamIdToSchema.get(outputStreamId)));
            // The query output is used for new queries
            if (!pCollectionTuple.has(outputStreamName)) {
                pCollectionTuple = pCollectionTuple.and(outputStreamName, queryOutputToBeFedBack);
            }
            PCollection<KV<String, byte[]>> collectionBytes = queryOutput
                    .apply(ParDo.of(new QueryDoFn(outputStreamName, schema)));

            for (int node_id : streamIdToNodeIds.getOrDefault(outputStreamId, new ArrayList<>())) {
                String topic = outputStreamName + "-" + node_id;
                //System.out.println("Setting bootstrap server node ID " + node_id + ": " + nodeIdToIpAndPort.get(node_id).get("ip") + ":9092");
                collectionBytes.apply(KafkaIO.<String, byte[]>write()
                    .withBootstrapServers(nodeIdToIpAndPort.get(node_id).get("ip") + ":9092")
                    .withTopic(topic)

                    .withKeySerializer(StringSerializer.class)
                    .withValueSerializer(ByteArraySerializer.class)

                    // you can further customize KafkaProducer used to write the records by adding more
                    // settings for ProducerConfig. e.g, to enable compression :
                    .updateProducerProperties(ImmutableMap.of("compression.type", "gzip"))

                    // Optionally enable exactly-once sink (on supported runners). See JavaDoc for withEOS().
                    //.withEOS(20, "eos-sink-group-id")
                );
            }
        }
    }

    @Override
    public String DeployQueries(Map<String, Object> query) {
        int outputStreamId = (int) query.get("output-stream-id");
        printDataStream.put(outputStreamId, (boolean) query.getOrDefault("print", false));
        tf.traceEvent(221, new Object[]{query.get("id")});
        fetchQueries.add(query);
        DeployQueries();
        return "Success";
    }

    boolean startedOnce = false;
    @Override
    public String StartRuntimeEnv() {
        if (startedOnce) {
            return "Success";
        }
        if (interrupted) {
            System.out.println("Still waiting for the runtime environment to interrupt!");
            return "Error, runtime environment has not exited from its previous execution";
        }

        if (threadRunningEnvironment != null && threadRunningEnvironment.isAlive()) {
            //throw new RuntimeException("The execution environment is already running. " +
            //	                   	   "Stop it with stopRuntimeEnv before running it again.");
            //System.out.println("The execution environment is already running. " +
            //	"Stop it with stopRuntimeEnv before running it again.");
            return "Environment already running";
        }
        //deployedQueries = 0;
        //outputStreamIdToFetchQueries.clear();
        //outputStreamIdToUpdateQueries.clear();
        threadRunningEnvironment = new Thread(() -> {
            //System.out.println("Starting environment");
            timeLastRecvdTuple = 0;
            number_received = 0;
            try {
                pipeline.run().waitUntilFinish();
            } catch (Exception e) {
                if (!interrupted) {
                    // This interrupt was not because of us
                    e.printStackTrace();
                }
                System.out.println("Stopping the execution environment");
            }
            timeLastRecvdTuple = 0;
            number_received = 0;
        });
        System.out.println("Starting runtime");
        threadRunningEnvironment.start();
        startedOnce = true;
        return "Success";
    }

    boolean interrupted = false;
    static int cnt3 = 0;
    @Override
    public String StopRuntimeEnv() {
        tf.traceEvent(101);
//        threadRunningEnvironment.interrupt();
//        threadRunningEnvironment = null;

        //Kafka09Fetcher.timeLastRecvdTuple = 0;
        timeLastRecvdTuple = 0;
        number_received = 0;
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(14);
        }
        return "Success";
    }

	/*@Override
	public String StopRuntimeEnv() {
		tf.traceEvent(101);
		threadRunningEnvironment.interrupt();
		threadRunningEnvironment = null;

		for (int stream_id : streamIdToDataStream.keySet()) {
			DataStream<Row> ds = streamIdToDataStream.get(stream_id);
			ds.addSink(regularSinkFunctions.get(stream_id));
		}

		tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(14);
		}
		File file = new File(System.getenv("FLINK_BINARIES") + "/log/FlinkWorker.log");
		try {
			// Empty contents of the log file
			new PrintWriter(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			Runtime.getRuntime().exec(System.getenv("FLINK_BINARIES") + "/bin/cancel-jobs.sh");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "Success";
	}*/

    @Override
    public String AddDataset(Map<String, Object> ds) {
        String datasetType = (String) ds.get("type");
        int stream_id = (int) ds.get("stream-id");

        if (datasetType.equals("csv")) {
			/*String file_name = (String) ds.get("file");
			RowTypeInfo typeInfo = streamIdToTypeInfo.get(stream_id);
			TupleCsvInputFormat<Row> inputFormat = new RowCsvInputFormat(new Path(file_name), typeInfo);
			DataSet<Row> csv = new DataSource<>(execenv, inputFormat, typeInfo, Utils.getCallLocationName());
			try {
				ArrayList<Row> tuples = (ArrayList<Row>) csv.collect();
				allPackets = tuples;
				streamToTuples.put(stream_id, tuples);
				csv.print();
			} catch (Exception e) {
				e.printStackTrace();
			}*/
        } else if (ds.get("type").equals("yaml")) {
            Map<String, Object> map = GetMapFromYaml(ds);
            ArrayList<Map<String, Object> > tuples = (ArrayList<Map<String, Object> >) map.get("cepevents");
            for (Map<String, Object> tuple : tuples) {
                AddTuples(tuple, 1);
            }
        } else {
            throw new RuntimeException("Invalid dataset type for dataset with Id " + ds.get("id"));
        }
        return "Success";
    }

    private Map<String, Object> GetMapFromYaml(Map<String, Object> ds) {
        FileInputStream fis = null;
        Yaml yaml = new Yaml();
        String dataset_path = System.getenv().get("EXPOSE_PATH") + "/" + ds.get("file");
        try {
            fis = new FileInputStream(dataset_path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(15);
        }
        return (Map<String, Object>) yaml.load(fis);
    }

    int tupleCnt = 0;
    @Override
    public String ProcessTuples(int number_tuples) {
        return "Success";
    }

    @Override
    public String ClearQueries() {
        tf.traceEvent(222);
        queries.clear();
        return "Success";
    }

    @Override
    public String ClearTuples() {
        return "Success";
    }

    @Override
    public String EndExperiment() {
        tf.writeTraceToFile(this.trace_output_folder, this.getClass().getSimpleName());
        return "Success";
    }

    @Override
    public String AddTpIds(List<Object> tracepointIds) {
        for (int tracepointId : (List<Integer>) (List<?>) tracepointIds) {
            tf.addTracepoint(tracepointId);
        }
        return "Success";
    }

	/* Cluster version of the RetEndOfStream method
	@Override
	public String RetEndOfStream(int milliseconds) {
		milliseconds += TIMELASTRECEIVEDTHRESHOLD;  // We add waiting time because we don't log every received tuple
		File file = new File(System.getenv("FLINK_BINARIES") + "/log/FlinkWorker.log");
		try {
			// First we wait until the log file is not empty
			// Importantly, the log file may only contain the logs for when having received tuples
			long firstLength = file.length();
			while (file.length() == 0) {
				firstLength = file.length();
				Thread.sleep(milliseconds);
			}
			while (file.length() != firstLength) {
				firstLength = file.length();
				Thread.sleep(milliseconds);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return "Success";
	}*/

    @Override
    public String RetEndOfStream(int milliseconds) {
        long time_diff = 0;
        do {
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(16);
            }
            long cur_time = System.currentTimeMillis();
            time_diff = cur_time - timeLastRecvdTuple;
            System.out.println("RetEndOfStream, time_diff: " + time_diff + ", cur-time: " + cur_time + ", timeLastRecvdTuple: " + timeLastRecvdTuple + ", number received: " + number_received);
        } while (time_diff < milliseconds || timeLastRecvdTuple == 0);
        return Long.toString(time_diff);
    }

    @Override
    public String TraceTuple(int tracepointId, List<String> arguments) {
        System.out.println("TraceTuple, tracepointId: " + tracepointId + ", arguments: " + arguments);
        tf.traceEvent(tracepointId, arguments.toArray());
        System.out.println("After tracing");
        return "Success";
    }

    @Override
    public String Configure() {
        for (Map<String, Object> schema : allSchemas.values()) {
            if ((boolean) schema.getOrDefault("intermediary-stream", false)) {
                continue;
            }
            AddKafkaConsumer(schema);
        }

        for (int stream_id : streamIdToKafkaCollection.keySet()) {
            PCollection<Row> kafkaCollection = streamIdToKafkaCollection.get(stream_id);
            kafkaCollection.setCoder(RowCoder.of(streamIdToSchema.get(stream_id)));
            String stream_name = streamIdToName.get(stream_id);

            if (pCollectionTuple == null) {
                pCollectionTuple = PCollectionTuple.of(stream_name, kafkaCollection);
            } else {
                pCollectionTuple = pCollectionTuple.and(stream_name, kafkaCollection);
            }
        }
        return "Success";
    }

    public static void main(String[] args) {
        boolean continue_running = true;
        while (continue_running) {
            BeamExposeWrapper experimentAPI = new BeamExposeWrapper();
            SpeComm speComm = new SpeComm(args, experimentAPI);
            experimentAPI.setNodeId(speComm.GetNodeId());
            experimentAPI.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
            experimentAPI.setNodeId(speComm.GetNodeId());
            speComm.AcceptTasks();
        }
    }
}
