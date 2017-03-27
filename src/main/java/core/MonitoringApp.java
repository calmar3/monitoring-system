package core;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import key.LampKey;
import key.StreetKey;
import model.Lamp;
import model.Street;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-java-project-0.1.jar
 * From the CLI you can then run
 * 		./bin/flink run -c MonitoringApp target/flink-java-project-0.1.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class MonitoringApp {

    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    private static final String LAMP_TOPIC = "lampInfo";

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Unsafe Code
         * If we want to implement avl, this object will be loaded at start
         * from database
         */
        LampsAvl.getInstance().put(3L,3);
        LampsAvl.getInstance().put(2L,2);
        LampsAvl.getInstance().put(1L,1);

		//Da capire utilizzo
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */



		//read from KAFKA


		// configure the Kafka consumer
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
		kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
		kafkaProps.setProperty("group.id", "myGroup");

		// always read the Kafka topic from the start
		kafkaProps.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumer
		FlinkKafkaConsumer010<Lamp> consumer = new FlinkKafkaConsumer010<>(
				LAMP_TOPIC,          //kafka topic
				new LampSchema(),   //deserialization schema
				kafkaProps);        //consumer configuration


		// assign a timestamp extractor to the consumer
		//consumer.assignTimestampsAndWatermarks(new LampTSExtractor());



		// create a Lamp data stream

		DataStream<Lamp> lampStream = env.addSource(consumer);

		DataStream<Lamp> filteredById = lampStream.filter(new FilterByLamp()).setParallelism(1);

		WindowedStream windowedStream = filteredById.keyBy(new LampKey()).window(TumblingProcessingTimeWindows.of(Time.seconds(3)));

		SingleOutputStreamOperator outputStream = windowedStream.apply(new WindowFunction<Lamp, Lamp, Long, Window>() {
			public void apply (Long key,
							   Window window,
							   Iterable<Lamp> input,
							   Collector<Lamp> out) throws Exception {
				double totalConsumption = 0;
				int n = 0;
				Lamp lamp = null;
				for (Lamp tempLamp: input) {
				    if (lamp == null)
				        lamp = tempLamp;
					totalConsumption += tempLamp.getConsumption();
					n++;
				}
				out.collect(new Lamp(key, totalConsumption/n,lamp.getAddress()));
			}
		});


		outputStream.print();

		WindowedStream streetWindowedStream = outputStream.keyBy(new StreetKey()).window(TumblingProcessingTimeWindows.of(Time.seconds(3)));

		SingleOutputStreamOperator streetOutputStream = streetWindowedStream.apply(new WindowFunction<Lamp, Street, String, Window>() {
			public void apply (String key,
							   Window window,
							   Iterable<Lamp> input,
							   Collector<Street> out) throws Exception {
				double totalConsumption = 0;
				int n = 0;
				for (Lamp tempLamp: input) {
					totalConsumption += tempLamp.getConsumption();
					n++;
				}
				out.collect(new Street(key, totalConsumption/n));
			}
		});

		streetOutputStream.print();

		/*

		DataStream<Lamp> lampsWithAvgCons = lampStream.keyBy(new LampKey()).reduce(new ReduceFunction<Lamp>() {
			@Override
			public Lamp reduce(Lamp l1, Lamp l2) {
				Lamp temp = new Lamp();
				temp.setId(l1.getId());
				temp.setConsumption((l1.getConsumption() + l2.getConsumption()) / 2 );
				return temp;
			}
		});



		lampsWithAvgCons.print();

*/







        /*

        ESEMPIO DI PROVA CON TUPLE

		DataStream<Tuple2<Integer, Double>> stream = env.fromElements(new Tuple2<Integer,Double>(new Integer(1),12.5), new Tuple2<Integer,Double>(new Integer(2),15.7), new Tuple2<Integer,Double>(new Integer(3),11.1));


		DataStream<Tuple2<Integer, Double>> adaptConsumption = stream

                .flatMap(new FlatMapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {
			@Override
			public void flatMap(Tuple2<Integer, Double> lampInfo, Collector<Tuple2<Integer, Double>> out) throws Exception {
				lampInfo.f1 = lampInfo.f1*2;
			    out.collect(lampInfo);
			}
		});


		adaptConsumption.print()
        */








        //write to KAFKA
		/*
		List<Lamp> data = new ArrayList<>();
		data.add(new Lamp(1,10.3));
		data.add(new Lamp(2,11.7));
		data.add(new Lamp(3,12.1));


		DataStream<Lamp> stream = env.fromCollection(data);
        // write the filtered data to a Kafka sink
        stream.addSink(new FlinkKafkaProducer010<Lamp>(
                LOCAL_KAFKA_BROKER,
                LAMP_TOPIC,
                new LampSchema()
                ));

		System.out.println(stream.print().toString());
		*/
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}