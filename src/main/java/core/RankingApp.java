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
import model.Lamp;
import operator.filter.FilterByLamp;
import operator.filter.ThresholdFilter;
import operator.filter.UpdateGlobalRankFilter;
import operator.key.LampIdKey;
import operator.merger.RankMerger;
import operator.ranker.LampRanker;
import operator.time.LampTSExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import utils.connector.KafkaConfigurator;

import java.util.TreeSet;


/*valerio*/

public class RankingApp {
	
    private static final int MAX_RANK_SIZE = 3;
    private static final long THRESHOLD = 100;
    private static final long WINDOW_SIZE = 90000;
	
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();


		// get a kafka consumer
		FlinkKafkaConsumer010<Lamp> kafkaConsumer = KafkaConfigurator.kafkaConsumer("lamp_data");

		// assign a timestamp extractor to the consumer
		FlinkKafkaConsumerBase<Lamp> kafkaConsumerTS = kafkaConsumer.assignTimestampsAndWatermarks(new LampTSExtractor());

		// add source
		DataStream<Lamp> lampStream = env.addSource(kafkaConsumerTS);

		// filter data
		DataStream<Lamp> filteredById = lampStream.filter(new FilterByLamp());








		// filter data by threshold
		DataStream<Lamp> filteredByThreshold = filteredById.filter(new ThresholdFilter(THRESHOLD));
		
		//filteredByThreshold.writeAsText("debug1");

		filteredByThreshold.print();

		// grouping by lamp id and windowing the streamtime
		WindowedStream<Lamp, Long , TimeWindow> windowedStream = filteredByThreshold.keyBy(new LampIdKey()).timeWindow(Time.milliseconds(WINDOW_SIZE));
		
		// compute partial rank 
		SingleOutputStreamOperator<TreeSet<Lamp>> partialRank = windowedStream.apply(new LampRanker(MAX_RANK_SIZE));
		
		//partialRank.writeAsText("debug2");
		partialRank.print().setParallelism(1);
		
		// compute global rank
		DataStream<TreeSet<Lamp>> globalRank = partialRank.flatMap(new RankMerger(MAX_RANK_SIZE)).setParallelism(1);
		
		// filter not updated global rank
		DataStream<TreeSet<Lamp>> updateGlobalRank = globalRank.filter(new UpdateGlobalRankFilter(MAX_RANK_SIZE)).setParallelism(1);
		
		// publish result on Kafka topic
		KafkaConfigurator.rankKafkaProducer("rank", updateGlobalRank);











		env.execute("Monitoring System");
	}
}