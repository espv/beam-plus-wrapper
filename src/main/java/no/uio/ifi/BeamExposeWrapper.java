package no.uio.ifi;

import no.uio.ifi.ExperimentAPI;
import no.uio.ifi.SpeComm;
import no.uio.ifi.TracingFramework;

import java.io.Serializable;
import java.util.logging.Logger;
import java.nio.file.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


@SuppressWarnings("unchecked")
public class BeamExposeWrapper implements ExperimentAPI, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkExperimentFramework.class);
    static long timeLastRecvdTuple = 0;
    int batch_size;
    int pktsPublished;
    int interval_wait;
    final int TIMELASTRECEIVEDTHRESHOLD = 1000;  // ms
    boolean useRowtime = true;
    String trace_output_folder;
    StreamExecutionEnvironment env;
    StreamTableEnvironment tableEnv;
    EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
    Map<Integer, ArrayList<Row> > streamToTuples = new HashMap<>();
    Map<Integer, ArrayList<Map<String, Object>> > outputStreamIdToFetchQueries = new HashMap<>();
    Map<Integer, ArrayList<Map<String, Object>> > outputStreamIdToUpdateQueries = new HashMap<>();
    List<Map<String, Object>> fetchQueries = new ArrayList<>();
    ArrayList<Map<String, Object>> queries = new ArrayList<>();
    Map<Integer, Map<String, Object>> allSchemas = new HashMap<>();
    Map<Integer, TypeInformation<Row>> streamIdToTypeInfo = new HashMap<>();
    Map<Integer, TypeInformationSerializationSchema<Row>> streamIdToSerializationSchema = new HashMap<>();
    Map<String, Integer> streamNameToId = new HashMap<>();
    Map<Integer, String> streamIdToName = new HashMap<>();
    Map<Integer, Map<String, Object>> nodeIdToIpAndPort = new HashMap<>();
    Map<Integer, List<Integer>> streamIdToNodeIds = new HashMap<>();
    Map<Integer, DataStream<Row> > streamIdToDataStream = new HashMap<>();
    Map<Integer, List<Map<String, Object>>> datasetIdToTuples = new HashMap<>();
    Map<Integer, Boolean> printDataStream = new HashMap<>();
    static TracingFramework tf = new TracingFramework();
    Producer<String, byte[]> producer;
    Map<Integer, SourceFunction<Row>> envSourceFunctions = new HashMap<>();

    List<FlinkKafkaProducer010<Row> > sinkFunctions = new ArrayList<>();
    Map<Integer, SinkFunction<Row> > regularSinkFunctions = new HashMap<>();
    Map<String, FlinkKafkaProducer010<Row> > querySinkFunctions = new HashMap<>();
    //List<SourceFunction<Row> > sourceFunctions = new ArrayList<>();
    Thread threadRunningEnvironment;
    static int nodeId;
    Properties props;
    Map<Integer, KafkaProducer> nodeIdToKafkaProducer = new HashMap<>();
    Map<Integer, Properties> nodeIdToProperties = new HashMap<>();
    List<Tuple2<Integer, Row>> all_tuples = new ArrayList<>();

    BeamExposeWrapper() {
        props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }

    public void setNodeId(int nodeId) {
        FlinkExperimentFramework.nodeId = nodeId;
        props.put("client.id", Integer.toString(nodeId));
        producer = new KafkaProducer<>(props);
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
            nodeIdToProperties.put(nodeId, properties);
            nodeIdToKafkaProducer.put(nodeId, new KafkaProducer<>(properties));
        }
        return "Success";
    }

    @Override
    public String AddNextHop(int streamId, int nodeId) {
        if (!streamIdToNodeIds.containsKey(streamId)) {
            streamIdToNodeIds.put(streamId, new ArrayList<>());
        }

        streamIdToNodeIds.get(streamId).add(nodeId);
        for (Map<String, Object> schema : allSchemas.values()) {
            if (schema.get("stream-id").equals(streamId)) {
                String stream_name = (String) schema.get("name");
                DataStream<Row> ds = streamIdToDataStream.get(streamId);
                if (ds == null) {
                    AddKafkaConsumer(schema);
                }
                ds = streamIdToDataStream.get(streamId);
                for (int otherNodeId : streamIdToNodeIds.get(streamId)) {
                    String topic = stream_name + "-" + otherNodeId;
                    FlinkKafkaProducer010<Row> p = new FlinkKafkaProducer010<>(topic, streamIdToSerializationSchema.get(streamId), nodeIdToProperties.get(otherNodeId));
                    sinkFunctions.add(p);
                    ds.addSink(p);
                }
                break;
            }
        }
        return "Success";
    }

    public String WriteStreamToCsv(int stream_id, String csvFolder) {
        Map<String, Object> schema = allSchemas.get(stream_id);
        assert schema != null;
        DataStream<Row> ds = streamIdToDataStream.get(stream_id);
        if (ds == null) {
            AddKafkaConsumer(schema);
        }
        ds = streamIdToDataStream.get(stream_id);
        int cnt = 1;
        boolean finished = false;
        while (!finished) {
            String path = System.getenv().get("EXPOSE_PATH") + "/" + csvFolder + "/flink/" + cnt;
            Path p = Paths.get(path);
            if (Files.exists(p)) {
                ++cnt;
                continue;
            }
            ds.writeAsText(path);
            finished = true;
        }
        return "Success";
    }

    volatile static int[] cnt = {0};

    private void AddKafkaConsumer(Map<String, Object> schema) {
        int stream_id = (int) schema.get("stream-id");
        String stream_name = (String) schema.get("name");
        String topic = stream_name + "-" + FlinkExperimentFramework.nodeId;
        System.out.println("Subscribing to kafka topic " + topic);
        FlinkKafkaConsumer010<Row> consumer = new FlinkKafkaConsumer010<>(topic, streamIdToSerializationSchema.get(stream_id), this.props, tf);
        //sourceFunctions.add(consumer);
        DataStream<Row> ds = env.addSource(consumer).returns(streamIdToTypeInfo.get(stream_id))/*.assignTimestampsAndWatermarks(
			new AscendingTimestampExtractor<Row>() {
				@Override
				public long extractAscendingTimestamp(Row element) {
					if (!schema.containsKey("rowtime-column")) {
						return 0;
					}
					List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
					for (int i = 0; i < element.getArity(); i++) {
						String attr_name = tuple_format.get(i).get("name");
						if (attr_name.equals("eventTime.rowtime")) {
							return ((Timestamp) element.getField(i)).getTime();
						}
					}
					return 0;
				}
			}).setParallelism(1);
		//ds.writeAsText("/home/espen/outputFromFlink.txt");
			})*/;
			/*new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.milliseconds(1)) {
			  @Override
			  public long extractTimestamp(Row element) {
				  if (!schema.containsKey("rowtime-column")) {
					  return 0;
				  }
				  List<Map<String, String>> tuple_format = (ArrayList<Map<String, String>>) schema.get("tuple-format");
				  for (int i = 0; i < element.getArity(); i++) {
					  if (tuple_format.get(i).get("name").equals(((Map<String, Object>) schema.get("rowtime-column")).get("column"))) {
						  return ((Timestamp) element.getField(i)).getTime();
					  }
				  }
				  return 0;
			  }
			});*/ /*new TimestampsAndWatermarks());*//*new
																																  AssignerWithPeriodicWatermarks<Row>() {
																																	  private long maxTimestampSeen = 0;
																																	  long cnt = 0;

																																	  @Override
																																	  public Watermark getCurrentWatermark() {
																																		  return new Watermark(maxTimestampSeen);
																																	  }

																																	  @Override
																																	  public long extractTimestamp(Row temperatureEvent, long l)
																																	  {
																																		  //long ts = temperatureEvent.getTimestamp();
																																		  // if (temperatureEvent.getKey().equals("W"))
																																		  //maxTimestampSeen = Long.max(maxTimestampSeen,ts);
																																		  //return ts;
																																		  cnt += 1000000000;
																																		  //System.out.println("Time: " + cnt + ", topic: " + topic);
																																		  return cnt;
																																	  }
																																  })*/ // Add as many fields as your Row has;
        //envSourceFunctions.put(stream_id, consumer);

        //if (nodeId == 4)
        //ds.print();
        SinkFunction<Row> sf = new SinkFunction<Row>() {
            private final Logger LOG = LoggerFactory.getLogger(SinkFunction.class);
            @Override
            public void invoke(Row value) throws Exception {
                long newTime = System.currentTimeMillis();
                ++cnt[0];
                // We only log once every maximum one second, to avoid too many tracepoints
                if (newTime - timeLastRecvdTuple > TIMELASTRECEIVEDTHRESHOLD) {
                    LOG.info("Received tuple {}", cnt[0]);
                    timeLastRecvdTuple = newTime;
                }
                //if (++cnt[0] % 10000 == 0)
                //System.out.println( System.nanoTime() + ": Received tuple " + (++cnt[0]));
            }
        };
        regularSinkFunctions.put(stream_id, sf);
        ds.addSink(sf);
        StringBuilder fields = new StringBuilder();
        for (Map<String, String> attribute : (List<Map<String, String> >) schema.get("tuple-format")) {
            if (!fields.toString().equals("")) {
                fields.append(", ");
            }

            fields.append(attribute.get("name"));
        }
        String fields_str = fields.toString();
        if (!useRowtime) {
            fields_str += ", eventTime.proctime";
        }
        tableEnv.registerDataStream(stream_name, ds, fields_str);
        this.streamIdToDataStream.put(stream_id, ds);
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
                        attribute.put("value", new Timestamp(Long.parseLong(attribute.get("value").toString())));
                        break;
                    case "timestamp":
                        attribute.put("value", Timestamp.valueOf(attribute.get("value").toString()));
                        break;
                    default:
                        throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }
        }
    }

    @Override
    public String SendDsAsStream(Map<String, Object> ds) {
        //System.out.println("Processing dataset");
        int ds_id = (int) ds.get("id");
        List<Map<String, Object>> tuples = datasetIdToTuples.get(ds_id);
        if (tuples == null) {
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

            // Sort the raw_tuples by their order
			/*raw_tuples.sort((lhs, rhs) -> {
				int lhs_order = (int) lhs.get("_order");
				int rhs_order = (int) rhs.get("_order");
				return Integer.compare(lhs_order, rhs_order);
			});*/

            datasetIdToTuples.put(ds_id, raw_tuples);
            tuples = raw_tuples;
        }
		/*double prevTimestamp = 0;
		//System.out.println("Ready to transmit tuples");
		long prevTime = System.nanoTime();
		boolean realism = (boolean) ds.getOrDefault("realism", false) && schema.containsKey("rowtime-column");
		for (Map<String, Object> tuple : tuples) {
			AddTuples(tuple, 1);

			if (realism) {
				Map<String, Object> rowtime_column = (Map<String, Object>) schema.get("rowtime-column");
				double timestamp = 0;
				for (Map<String, Object> attribute : (List<Map<String, Object>>) tuple.get("attributes")) {
					if (attribute.get("name").equals(rowtime_column.get("column"))) {
						int nanoseconds_per_tick = (int) rowtime_column.get("nanoseconds-per-tick");
						timestamp = (double) attribute.get("value") * nanoseconds_per_tick;
						if (prevTimestamp == 0) {
							prevTimestamp = timestamp;
						}
						break;
					}
				}
				double time_diff_tuple = timestamp - prevTimestamp;
				long time_diff_real = System.nanoTime() - prevTime;
				while (time_diff_real < time_diff_tuple) {
					time_diff_real = System.nanoTime() - prevTime;
				}

				prevTimestamp = timestamp;
				prevTime = System.nanoTime();
			}
		}

		if (!realism) {
			ProcessTuples(tuples.size());
		}*/

        for (Map<String, Object> tuple : tuples) {
            AddTuples(tuple, 1);
        }
        ProcessTuples(tuples.size());
        return "Success";
    }

    @Override
    public String AddTuples(Map<String, Object> tuple, int quantity) {
        int stream_id = (int) tuple.get("stream-id");
        ArrayList<Map<String, Object> > attributes = (ArrayList<Map<String, Object> >) tuple.get("attributes");
        Row new_tuple = new Row(attributes.size());
        for (int i = 0; i < attributes.size(); i++) {
            Map<String, Object> attribute = attributes.get(i);
            new_tuple.setField(i, attribute.get("value"));
        }
		/*int stream_id = (int) tuple.get("stream-id");
		if (!streamToTuples.containsKey(stream_id)) {
			streamToTuples.put(stream_id, new ArrayList<>());
		}

		for (int i = 0; i < quantity; i++) {
			streamToTuples.get(stream_id).add(new_tuple);
		}*/
        all_tuples.add(new Tuple2<Integer, Row>(stream_id, new_tuple));
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
            TypeInformation<?>[] typeInformations = new TypeInformation[tuple_format.size()];
            int pos = -1;
            String rowtime_column = null;
            if (useRowtime && schema.containsKey("rowtime-column")) {
                rowtime_column = ((Map<String, String>)schema.get("rowtime-column")).get("column");
            }
            for (Map<String, String> attribute : tuple_format) {
                pos += 1;
                if (useRowtime && attribute.get("name") != null && attribute.get("name").equals(rowtime_column)) {
                    attribute.put("name", "eventTime.rowtime");
                }
                switch (attribute.get("type")) {
                    case "string":
                        typeInformations[pos] = Types.STRING;
                        break;
                    case "bool":
                        typeInformations[pos] = Types.BOOLEAN;
                        break;
                    case "int":
                        typeInformations[pos] = Types.INT;
                        break;
                    case "float":
                        typeInformations[pos] = Types.FLOAT;
                        break;
                    case "double":
                        typeInformations[pos] = Types.DOUBLE;
                        break;
                    case "long":
                        typeInformations[pos] = Types.LONG;
                        break;
                    case "long-timestamp":
                    case "timestamp":
                        typeInformations[pos] = Types.SQL_TIMESTAMP;
                        break;
                    default:
                        throw new RuntimeException("Invalid attribute type in dataset definition");
                }
            }

            if (env == null) {
                env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.getConfig().disableSysoutLogging();
                if (useRowtime) {
                    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                } else {
                    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                }
                tableEnv = StreamTableEnvironment.create(env, fsSettings);
            }

            streamIdToTypeInfo.put(stream_id, new RowTypeInfo(typeInformations));
            streamIdToSerializationSchema.put(stream_id, new TypeInformationSerializationSchema<>(streamIdToTypeInfo.get(stream_id), env.getConfig()));
        }
        return "Success";
    }

    public void DeployQueries() {
        for (Map<String, Object> query : fetchQueries) {
            int outputStreamId = (int) query.get("output-stream-id");
            String outputStreamName = streamIdToName.get(outputStreamId);
            Map<String, Object> output_schema = this.allSchemas.get(outputStreamId);
            String sql_query = ((Map<String, String>) query.get("sql-query")).get("flink");
            Table result = tableEnv.sqlQuery(sql_query);
            if (!(boolean) output_schema.getOrDefault("registered", false) &&
                    (boolean) output_schema.getOrDefault("intermediary-stream", false)) {
                output_schema.put("registered", true);
                tableEnv.registerTable(this.streamIdToName.get(outputStreamId), result);
            } else {
                // Need to filter out the processing time element
                DataStream<Row> ds =
                        tableEnv.toRetractStream(result, streamIdToTypeInfo.get(outputStreamId))
                                .map((MapFunction<Tuple2<Boolean, Row>, Row>) value -> value.f1);
                if (streamIdToNodeIds.containsKey(outputStreamId)) {
                    for (int otherNodeId : streamIdToNodeIds.get(outputStreamId)) {
                        String topic = outputStreamName + "-" + otherNodeId;
                        FlinkKafkaProducer010<Row> p;
                        if (querySinkFunctions.containsKey(topic)) {
                            p = querySinkFunctions.get(topic);
                        } else {
                            p = new FlinkKafkaProducer010<>(topic,
                                    streamIdToSerializationSchema.get(outputStreamId),
                                    nodeIdToProperties.get(otherNodeId));
                            querySinkFunctions.put(topic, p);
                        }
                        ds.addSink(p);
                    }
                }
            }
        }
    }

    @Override
    public String DeployQueries(Map<String, Object> query) {
        int outputStreamId = (int) query.get("output-stream-id");
        printDataStream.put(outputStreamId, (boolean) query.getOrDefault("print", false));
        tf.traceEvent(221, new Object[]{query.get("id")});
        fetchQueries.add(query);
        return "Success";
    }

    @Override
    public String StartRuntimeEnv() {
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
        DeployQueries();
        //deployedQueries = 0;
        //outputStreamIdToFetchQueries.clear();
        //outputStreamIdToUpdateQueries.clear();
        threadRunningEnvironment = new Thread(() -> {
            //System.out.println("Starting environment");
            timeLastRecvdTuple = 0;
            try {
                env.execute();
            } catch (Exception e) {
                if (!interrupted) {
                    // This interrupt was not because of us
                    e.printStackTrace();
                }
                System.out.println("Stopping the execution environment");
            }
            timeLastRecvdTuple = 0;
        });
        interrupted = false;
        threadRunningEnvironment.start();
        return "Success";
    }

    boolean interrupted = false;
    static int cnt3 = 0;
    @Override
    public String StopRuntimeEnv() {
        tf.traceEvent(101);
        threadRunningEnvironment.interrupt();
        threadRunningEnvironment = null;

        for (int stream_id : streamIdToDataStream.keySet()) {
            DataStream<Row> ds = streamIdToDataStream.get(stream_id);
            ds.addSink(regularSinkFunctions.get(stream_id));
        }

        Kafka09Fetcher.timeLastRecvdTuple = 0;
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
        return yaml.load(fis);
    }

    int tupleCnt = 0;
    @Override
    public String ProcessTuples(int number_tuples) {
        System.out.println("Processing " + number_tuples);
        for (Tuple2<Integer, Row> tuple : all_tuples) {
            int stream_id = tuple.f0;
            Row row = tuple.f1;
            TypeInformationSerializationSchema<Row> serializationSchema = streamIdToSerializationSchema.get(stream_id);
            String stream_name = (String) allSchemas.get(stream_id).get("name");
            for (int otherNodeId : streamIdToNodeIds.getOrDefault(stream_id, new ArrayList<>())) {
                String topicName = stream_name + "-" + otherNodeId;
                //for (Row tuple : streamToTuples.get(stream_id)) {
                if (++tupleCnt % 100000 == 0) {
                    System.out.println( System.nanoTime() + ": Sending tuple " + (++tupleCnt) + " to node " + otherNodeId + " with topic " + topicName);
                }
                this.nodeIdToKafkaProducer.get(otherNodeId).send(new ProducerRecord<>(topicName, serializationSchema.serialize(row)));
                //}
            }
        }


		/*for (int stream_id : streamToTuples.keySet()) {
			TypeInformationSerializationSchema<Row> serializationSchema = streamIdToSerializationSchema.get(stream_id);
			String stream_name = (String) allSchemas.get(stream_id).get("name");
			for (int otherNodeId : streamIdToNodeIds.getOrDefault(stream_id, new ArrayList<>())) {
				String topicName = stream_name + "-" + otherNodeId;
				for (Row tuple : streamToTuples.get(stream_id)) {
					if (++tupleCnt % 100000 == 0) {
						System.out.println( System.nanoTime() + ": Sending tuple " + (++tupleCnt) + " to node " + otherNodeId + " with topic " + topicName);
					}
					this.nodeIdToKafkaProducer.get(otherNodeId).send(new ProducerRecord<>(topicName, serializationSchema.serialize(tuple)));
				}
			}
		}
		streamToTuples.clear();*/
        all_tuples.clear();
        pktsPublished = 0;
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
            this.tf.addTracepoint(tracepointId);
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
        long time_diff;
        do {
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long cur_time = System.currentTimeMillis();
            time_diff = cur_time - Kafka09Fetcher.timeLastRecvdTuple;
            System.out.println("RetEndOfStream, time_diff: " + time_diff + ", cur-time: " + cur_time + ", timeLastRecvdTuple: " + Kafka09Fetcher.timeLastRecvdTuple);
        } while (time_diff < milliseconds || Kafka09Fetcher.timeLastRecvdTuple == 0);
        return Long.toString(time_diff);
    }

    @Override
    public String TraceTuple(int tracepointId, List<String> arguments) {
        System.out.println("TraceTuple, tracepointId: " + tracepointId + ", arguments: " + arguments);
        tf.traceEvent(tracepointId, arguments.toArray());
        System.out.println("After tracing");
        return "Success";
    }

    public String Configure() {
        for (Map<String, Object> schema : allSchemas.values()) {
            if ((boolean) schema.getOrDefault("intermediary-stream", false)) {
                continue;
            }
            AddKafkaConsumer(schema);
        }
        return "Success";
    }

    public static void main(String[] args) {
        boolean continue_running = true;
        while (continue_running) {
            FlinkExperimentFramework experimentAPI = new FlinkExperimentFramework();
            SpeComm speComm = new SpeComm(args, experimentAPI);
            experimentAPI.setNodeId(speComm.GetNodeId());
            experimentAPI.SetTraceOutputFolder(speComm.GetTraceOutputFolder());
            experimentAPI.setNodeId(speComm.GetNodeId());
            speComm.AcceptTasks();
        }
    }
}
