package core;

import control.AppConfigurator;
import control.EnvConfigurator;
import model.Lamp;
import operator.filter.*;
import operator.flatmap.RankMerger;
import operator.join.LocalGlobalMedianJoin;
import operator.key.LampAddressKey;
import operator.key.LampAddressKey2;
import operator.key.LampCityKey;
import operator.key.LampIdKey;
import operator.time.LampTSExtractor;
import operator.window.foldfunction.*;
import operator.window.windowfunction.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import utils.connector.KafkaConfigurator;

public class MonitoringApp {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();

		AppConfigurator.readConfiguration();

        /*
         * Unsafe Code
         * If we want to implement avl, this object will be loaded at start
         * from database
         */
		//LampsAvl.getInstance().put(3L,3);
		//LampsAvl.getInstance().put(2L,2);
		//LampsAvl.getInstance().put(1L,1);


		// get a kafka consumer
		FlinkKafkaConsumer010<Lamp> kafkaConsumer = KafkaConfigurator.kafkaConsumer(AppConfigurator.LAMP_DATA_TOPIC);

		// assign a timestamp extractor to the consumer
		FlinkKafkaConsumerBase<Lamp> kafkaConsumerTS = kafkaConsumer.assignTimestampsAndWatermarks(new LampTSExtractor());;

		// add source
		DataStream<Lamp> lampStream = env.addSource(kafkaConsumerTS).setParallelism(1);

		// filter data
		DataStream<Lamp> filteredById = lampStream.filter(new LampFilter());


		/**
		 * Data for test configuration parameters
		 */
/*
		ObjectMapper mapper = new ObjectMapper();

		List<Lamp> data = mapper.readValue(new File(AppConfigurator.DATASET_FILE), new TypeReference<List<Lamp>>() {
		});

		int value = 10;

		long lastSubDate = System.currentTimeMillis();
		for (int i = 0; i < data.size(); i++) {
			data.get(i).setConsumption((i + 1) % 10 != 0 ? value + (i / 10) : value*2.5  + (i / 10));
			data.get(i).setTimestamp(System.currentTimeMillis());
			data.get(i).setResidualLifeTime(data.get(i).getTimestamp() - data.get(i).getLastSubstitutionDate());
			if((i+1)%100 == 1)
				data.get(i).setStateOn(false);
		}

		for(int k = 1; k < AppConfigurator.ADD_TUPLE_FOR_TEST; k++) {
			for (int i = 0; i < 1000; i++) {
				data.add(data.get(i));
				data.get(i + k*1000).setTimestamp(System.currentTimeMillis() + k*10000);
				data.get(i + k*1000).setResidualLifeTime(data.get(i + k*1000).getTimestamp() - data.get(i).getLastSubstitutionDate());
				if((i+1)%100 == 1)
					data.get(i + k*1000).setStateOn(false);
			}
		}

		DataStream<Lamp> lampStream = env.fromCollection(data).assignTimestampsAndWatermarks(new LampTSExtractor()).setParallelism(1);

		DataStream<Lamp> filteredById = lampStream.filter(new LampFilter()).setParallelism(1);
*/

		/**
		 * Warning for lamp stateOn
		 */
		DataStream<Lamp> warningState = filteredById.filter(new StateOnFilter());
		KafkaConfigurator.lampKafkaProducer(AppConfigurator.WARNING_STATE, warningState);
		//warningState.print();

		/**
		 * INSERT CODE FOR RANKING
		 *
		 */
		// filter data by threshold
		DataStream<Lamp> filteredByThreshold = filteredById.filter(new ThresholdFilter(AppConfigurator.THRESHOLD)).setParallelism(1);

		// grouping by lamp id and windowing the stream
		WindowedStream rankWindowedStream = filteredByThreshold.keyBy(new LampIdKey()).timeWindow(Time.seconds(AppConfigurator.RANK_WINDOW_SIZE));

		// compute partial rank
		SingleOutputStreamOperator partialRank = rankWindowedStream.apply(new LampRankerWF(AppConfigurator.MAX_RANK_SIZE));
		//partialRank.print();


		// compute global rank
		DataStream globalRank = partialRank.flatMap(new RankMerger(AppConfigurator.MAX_RANK_SIZE)).setParallelism(1);
		//globalRank.print();

		// filter not updated global rank
		DataStream updateGlobalRank = globalRank.filter(new UpdateGlobalRankFilter(AppConfigurator.MAX_RANK_SIZE)).setParallelism(1);
		//updateGlobalRank.print();

		// publish result on Kafka topic
		KafkaConfigurator.rankKafkaProducer(AppConfigurator.RANK_TOPIC, updateGlobalRank);


		/**
		 * AVG CONSUMPTION FOR LAMP
		 */
		// average consumption for lamp (hour)
		WindowedStream lampWindowedStreamHour = filteredById.keyBy(new LampIdKey()).timeWindow(Time.seconds(AppConfigurator.HOUR_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.HOUR_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsLampStreamHour = lampWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new AvgConsLampFF(), new AvgLampWF());
		//avgConsLampStreamHour.print();
		KafkaConfigurator.lampKafkaProducer(AppConfigurator.HOUR_LAMP_CONS, avgConsLampStreamHour);


		// average consumption for lamp (day)
		WindowedStream lampWindowedStreamDay = avgConsLampStreamHour.keyBy(new LampIdKey()).timeWindow(Time.seconds(AppConfigurator.DAY_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.DAY_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsLampStreamDay = lampWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new AvgConsLampFF(), new AvgLampWF());
		//avgConsLampStreamDay.print();
		KafkaConfigurator.lampKafkaProducer(AppConfigurator.DAY_LAMP_CONS, avgConsLampStreamDay);


		// average consumption for lamp (week)
		WindowedStream lampWindowedStreamWeek = avgConsLampStreamDay.keyBy(new LampIdKey()).timeWindow(Time.seconds(AppConfigurator.WEEK_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.WEEK_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsLampStreamWeek = lampWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new AvgConsLampFF(), new AvgLampWF());
		//avgConsLampStreamWeek.print();
		KafkaConfigurator.lampKafkaProducer(AppConfigurator.WEEK_LAMP_CONS, avgConsLampStreamWeek);


		/**
		 * AVG CONSUMPTION FOR STREET
		 */
		// average consumption by street (hour)
		WindowedStream streetWindowedStreamHour = avgConsLampStreamHour.keyBy(new LampAddressKey()).timeWindow(Time.seconds(AppConfigurator.HOUR_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.HOUR_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsStreetStreamHour = streetWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new AvgConStreetWarnFF(AppConfigurator.WARNING_HOUR_TOPIC), new AvgStreetWF());
		//avgConsStreetStreamHour.print();
		KafkaConfigurator.streetKafkaProducer(AppConfigurator.HOUR_STREET_CONS, avgConsStreetStreamHour);


		// average consumption by street (day)
		WindowedStream streetWindowedStreamDay = avgConsLampStreamDay.keyBy(new LampAddressKey()).timeWindow(Time.seconds(AppConfigurator.DAY_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.DAY_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsStreetStreamDay = streetWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new AvgConStreetWarnFF(AppConfigurator.WARNING_DAY_TOPIC), new AvgStreetWF());
		//avgConsStreetStreamDay.print();
		KafkaConfigurator.streetKafkaProducer(AppConfigurator.DAY_STREET_CONS, avgConsStreetStreamDay);


		WindowedStream streetWindowedStreamWeek = avgConsLampStreamWeek.keyBy(new LampAddressKey()).timeWindow(Time.seconds(AppConfigurator.WEEK_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.WEEK_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsStreetStreamWeek = streetWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new AvgConStreetWarnFF(AppConfigurator.WARNING_WEEK_TOPIC), new AvgStreetWF());
		//avgConsStreetStreamWeek.print();
		KafkaConfigurator.streetKafkaProducer(AppConfigurator.WEEK_STREET_CONS, avgConsStreetStreamWeek);


		/**
		 * AVG CONSUMPTION FOR CITY
		 */
		// global average consumption
		AllWindowedStream cityWindowedStreamHour = avgConsStreetStreamHour.timeWindowAll(Time.seconds(AppConfigurator.HOUR_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.HOUR_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsCityStreamHour = cityWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new AvgConsCityFF(), new AvgCityWF()).setParallelism(1);
		//avgConsCityStreamHour.print();
		KafkaConfigurator.cityKafkaProducer(AppConfigurator.HOUR_CITY_CONS, avgConsCityStreamHour);


		AllWindowedStream cityWindowedStreamDay = avgConsStreetStreamDay.timeWindowAll(Time.seconds(AppConfigurator.DAY_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.DAY_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsCityStreamDay = cityWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new AvgConsCityFF(), new AvgCityWF()).setParallelism(1);
		//avgConsCityStreamDay.print();
		KafkaConfigurator.cityKafkaProducer(AppConfigurator.DAY_CITY_CONS, avgConsCityStreamDay);

		AllWindowedStream cityWindowedStreamWeek = avgConsStreetStreamWeek.timeWindowAll(Time.seconds(AppConfigurator.WEEK_CONS_WINDOW_SIZE), Time.seconds(AppConfigurator.WEEK_CONS_WINDOW_SLIDE));
		SingleOutputStreamOperator avgConsCityStreamWeek = cityWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new AvgConsCityFF(), new AvgCityWF()).setParallelism(1);
		//avgConsCityStreamWeek.print();
		KafkaConfigurator.cityKafkaProducer(AppConfigurator.WEEK_CITY_CONS, avgConsCityStreamWeek);


		/**
		 * 50 MEDIAN
		 */
		WindowedStream LampWindowedStream = filteredById.keyBy(new LampIdKey()).timeWindow(Time.seconds(AppConfigurator.MEDIAN_WINDOW_SIZE), Time.seconds(AppConfigurator.MEDIAN_WINDOW_SLIDE));
		SingleOutputStreamOperator lampMedianStream = LampWindowedStream.fold(new Tuple2<>(null, null), new MedianConsLampFF(), new MedianLampWF());
		//lampMedianStream.print();

		AllWindowedStream globalWindowedStream = lampMedianStream.timeWindowAll(Time.seconds(AppConfigurator.MEDIAN_WINDOW_SIZE),  Time.seconds(AppConfigurator.MEDIAN_WINDOW_SLIDE));
		SingleOutputStreamOperator globalMedianStream = globalWindowedStream.fold(new Tuple2<>(null, null), new MedianConsLampFF(), new MedianGlobalWF());
		//globalMedianStream.print();

		JoinedStreams.WithWindow joinedWindowedStream = lampMedianStream.join(globalMedianStream).where(new LampCityKey()).equalTo(new LampCityKey()).window(TumblingEventTimeWindows.of(Time.seconds(AppConfigurator.MEDIAN_WINDOW_SLIDE)));
		DataStream joinedMedianStream = joinedWindowedStream.apply(new LocalGlobalMedianJoin());

		WindowedStream groupedStreet = joinedMedianStream.keyBy(new LampAddressKey2()).timeWindow(Time.seconds(AppConfigurator.MEDIAN_WINDOW_SIZE*2));
		SingleOutputStreamOperator percentualForStreet = groupedStreet.fold(new Tuple3<>(null, (long) 0,(long) 0), new MedianCountForPercentualFF(), new MedianPercentualWF());
		//percentualForStreet.print();

		SingleOutputStreamOperator filteredPercForStreet = percentualForStreet.keyBy("f0").filter(new PercentualFilter()).setParallelism(1);
		//filteredPercForStreet.print();
		KafkaConfigurator.medianKafkaProducer(AppConfigurator.MEDIAN_TOPIC, filteredPercForStreet);


		env.execute("Monitoring System");

		//JobExecutionResult res = env.execute("Monitoring");
		//PerformanceWriter.write(res, "/Users/maurizio/Desktop/TestParallelism.txt", data.size(), env.getParallelism());
	}
}