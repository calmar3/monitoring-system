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

import control.EnvConfigurator;
import operator.window.AvgConsumptionGlobal;
import operator.window.AvgConsumptionLamp;
import operator.window.AvgConsumptionStreet;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import utils.connector.KafkaConfigurator;
import operator.filter.FilterByLamp;
import operator.key.LampKey;
import operator.key.StreetKey;
import model.Lamp;
import model.Street;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import utils.structure.LampsAvl;


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

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();

        /*
         * Unsafe Code
         * If we want to implement avl, this object will be loaded at start
         * from database
         */
        LampsAvl.getInstance().put(3L,3);
        LampsAvl.getInstance().put(2L,2);
        LampsAvl.getInstance().put(1L,1);


		// get a kafka consumer
		FlinkKafkaConsumer010<Lamp> kafkaConsumer = KafkaConfigurator.getConsumer();


		// assegnare timestamp e watermarks su kafka

		/*
		kafkaConsumer.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Lamp>() {
			@Override
			public long extractAscendingTimestamp(Lamp lamp) {
				return lamp.eventTime();
			}
		});
		*/


		// add source
		DataStream<Lamp> lampStream = env.addSource(kafkaConsumer);


		// filter data
		DataStream<Lamp> filteredById = lampStream.filter(new FilterByLamp()).setParallelism(1);



		// average consumption by lampId
		WindowedStream windowedStream = filteredById.keyBy(new LampKey()).window(TumblingProcessingTimeWindows.of(Time.seconds(3)));

		SingleOutputStreamOperator outputStream = windowedStream.apply(new AvgConsumptionLamp());

		outputStream.print();





		// average consumption by street
		WindowedStream streetWindowedStream = outputStream.keyBy(new StreetKey()).window(TumblingProcessingTimeWindows.of(Time.seconds(3)));

		SingleOutputStreamOperator streetOutputStream = streetWindowedStream.apply(new AvgConsumptionStreet());

		streetOutputStream.print();





		// global average consumption
		AllWindowedStream cityWindowedStream = streetOutputStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

		SingleOutputStreamOperator cityOutputStream = cityWindowedStream.apply(new AvgConsumptionGlobal());

		cityOutputStream.print();



		/**
		 * In questa parte finale ci andrebbe la parte di codice presente al momento
		 * nella classe WriteToKafkaApp che per i test genera il flusso in ingresso a
		 * questo stream all'inizio di questa classe, valutare se scrivere su diversi topic
		 * kafka, al momento scrittura sull'unico topic lampInfo
		 */

		env.execute("Flink Streaming Java API Skeleton");
	}
}