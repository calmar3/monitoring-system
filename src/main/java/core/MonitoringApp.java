package core;

import control.EnvConfigurator;
import model.Street;
import operator.filter.*;
import operator.join.LocalGlobalMedianJoin;
import operator.key.*;
import operator.flatmap.RankMerger;
import operator.window.windowfunction.LampRankerWF;
import operator.time.LampTSExtractor;
import operator.window.foldfunction.*;
import operator.window.windowfunction.*;
import org.apache.avro.data.Json;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import utils.connector.KafkaConfigurator;
import model.Lamp;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.serialization.JsonEncoder;

import java.util.TreeSet;

public class MonitoringApp {

	// kafka topic
	private static final String LAMP_DATA_TOPIC = "lamp_data";

	private static final String RANK_TOPIC = "rank";
	private static final String WARNING_HOUR_TOPIC = "warning_hour";
	private static final String WARNING_DAY_TOPIC = "warning_day";
	private static final String WARNING_WEEK_TOPIC = "warning_week";
	private static final String WARNING_STATE = "warning_state";

	private static final String HOUR_LAMP_CONS = "hour_lamp_cons";
	private static final String DAY_LAMP_CONS = "day_lamp_cons";
	private static final String WEEK_LAMP_CONS = "week_lamp_cons";

	private static final String HOUR_STREET_CONS = "hour_street_cons";
	private static final String DAY_STREET_CONS = "day_street_cons";
	private static final String WEEK_STREET_CONS = "week_street_cons";

	private static final String HOUR_CITY_CONS = "hour_city_cons";
	private static final String DAY_CITY_CONS = "day_city_cons";
	private static final String WEEK_CITY_CONS = "week_city_cons";

	// ranking
	private static final int MAX_RANK_SIZE = 3;
	private static final long THRESHOLD = 1000;
	private static final long RANK_WINDOW_SIZE = 10; //seconds

	// avg Consumption
	private static final long HOUR_CONS_WINDOW_SIZE = 60; //minutes
	private static final long HOUR_CONS_WINDOW_SLIDE = 10; //minutes
	private static final long DAY_CONS_WINDOW_SIZE = 24; //hours
	private static final long DAY_CONS_WINDOW_SLIDE = 4; //hours
	private static final long WEEK_CONS_WINDOW_SIZE = 7; //days
	private static final long WEEK_CONS_WINDOW_SLIDE = 1; //days

	// median
	private static final long MEDIAN_WINDOW_SIZE = 10; //seconds
	private static final long MEDIAN_WINDOW_SLIDE = 2; //seconds


	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();

        /*
         * Unsafe Code
         * If we want to implement avl, this object will be loaded at start
         * from database
         */
        //LampsAvl.getInstance().put(3L,3);
        //LampsAvl.getInstance().put(2L,2);
        //LampsAvl.getInstance().put(1L,1);

		// get a kafka consumer
		FlinkKafkaConsumer010<Lamp> kafkaConsumer = KafkaConfigurator.kafkaConsumer(LAMP_DATA_TOPIC);

		// assign a timestamp extractor to the consumer
		FlinkKafkaConsumerBase<Lamp> kafkaConsumerTS = kafkaConsumer.assignTimestampsAndWatermarks(new LampTSExtractor());

		// add source
		DataStream<Lamp> lampStream = env.addSource(kafkaConsumerTS);

		// filter data
		DataStream<Lamp> filteredById = lampStream.filter(new LampFilter());

		/**
		 * Data for validate median calculation
		*/
/*
		List<Lamp> data = new ArrayList<>();

		for(long i=1; i<=5; i++){
			data.add(new Lamp(1, i*1100, "Roma", "via palmiro togliatti", i*10));
			data.add(new Lamp(2, i*1200, "Roma", "via palmiro togliatti", i*20));
			data.add(new Lamp(3, i*1300, "Roma", "via palmiro togliatti", i*30));
			data.add(new Lamp(4, i*1400, "Roma", "via tuscolana", i*40));
		}

		DataStream<Lamp> lampStream = env.fromCollection(data).assignTimestampsAndWatermarks(new LampTSExtractor());

		lampStream.writeAsText("Data");
*/
		/**
		 * Warning for lamp stateOn
		 */
		DataStream<Lamp> warningState = filteredById.filter(new StateOnFilter());
		KafkaConfigurator.lampKafkaProducer(WARNING_STATE, warningState);


		/**
		 * INSERT CODE FOR RANKING
		 *
		 */

		// filter data by threshold
		DataStream<Lamp> filteredByThreshold = filteredById.filter(new ThresholdFilter(THRESHOLD));

		//filteredByThreshold.writeAsText("debug1");

		// grouping by lamp id and windowing the stream
		WindowedStream<Lamp, Long , TimeWindow> rankWindowedStream = filteredByThreshold.keyBy(new LampIdKey()).timeWindow(Time.seconds(RANK_WINDOW_SIZE));

		// compute partial rank
		SingleOutputStreamOperator<TreeSet<Lamp>> partialRank = rankWindowedStream.apply(new LampRankerWF(MAX_RANK_SIZE));

		//partialRank.writeAsText("debug2");

		// compute global rank
		DataStream<TreeSet<Lamp>> globalRank = partialRank.flatMap(new RankMerger(MAX_RANK_SIZE)).setParallelism(1);

		// filter not updated global rank
		DataStream<TreeSet<Lamp>> updateGlobalRank = globalRank.filter(new UpdateGlobalRankFilter(MAX_RANK_SIZE)).setParallelism(1);

		// publish result on Kafka topic
		KafkaConfigurator.rankKafkaProducer(RANK_TOPIC, updateGlobalRank);


		/**
		 * AVG CONSUMPTION FOR LAMP
		 */

		// average consumption for lamp (hour)
		WindowedStream lampWindowedStreamHour = filteredById.keyBy(new LampIdKey()).timeWindow(Time.hours(1), Time.minutes(10));
		SingleOutputStreamOperator avgConsLampStreamHour = lampWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new AvgConsLampFF(), new AvgLampWF());
		avgConsLampStreamHour.print();
		//KafkaConfigurator.lampKafkaProducer(HOUR_LAMP_CONS, avgConsLampStreamHour);
/*
		// average consumption for lamp (day)
		WindowedStream lampWindowedStreamDay = avgConsLampStreamHour.keyBy(new LampIdKey()).timeWindow(Time.minutes(3));
		SingleOutputStreamOperator avgConsLampStreamDay = lampWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new AvgConsLampFF(), new AvgLampWF());
		avgConsLampStreamDay.print();
		//KafkaConfigurator.lampKafkaProducer(DAY_LAMP_CONS, avgConsLampStreamDay);
*/
/*		// average consumption for lamp (week)
		WindowedStream lampWindowedStreamWeek = avgConsLampStreamDay.keyBy(new LampIdKey()).timeWindow(Time.minutes(5));
		SingleOutputStreamOperator<Tuple2<Lamp, Double>> avgConsLampStreamWeek = lampWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new AvgConsLampFF(), new AvgLampWF());
		avgConsLampStreamWeek.print();
		//KafkaConfigurator.lampKafkaProducer(WEEK_LAMP_CONS, avgConsLampStreamWeek);
*/

		/**
		 * AVG CONSUMPTION FOR STREET
		 */
		// average consumption by street (hour)
		WindowedStream streetWindowedStreamHour = avgConsLampStreamHour.keyBy(new LampAddressKey()).timeWindow(Time.hours(1));
		DataStream<Street> avgConsStreetStreamHour = streetWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new AvgConStreetWarnFF(WARNING_HOUR_TOPIC), new AvgStreetWF());
		avgConsStreetStreamHour.print();
		KafkaConfigurator.streetKafkaProducer(HOUR_STREET_CONS, avgConsStreetStreamHour);

/*
		// average consumption by street (day)
		WindowedStream streetWindowedStreamDay = avgConsLampStreamDay.keyBy(new LampAddressKey()).timeWindow(Time.minutes(3));
		DataStream<Street> avgConsStreetStreamDay = streetWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new AvgConStreetWarnFF(WARNING_DAY_TOPIC), new AvgStreetWF());
		avgConsStreetStreamDay.print();
		KafkaConfigurator.streetKafkaProducer(DAY_STREET_CONS, avgConsStreetStreamDay);


		WindowedStream streetWindowedStreamWeek = avgConsLampStreamWeek.keyBy(new LampAddressKey()).timeWindow(Time.minutes(5));
		DataStream<Street> avgConsStreetStreamWeek = streetWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new AvgConStreetWarnFF(WARNING_WEEK_TOPIC), new AvgStreetWF());
		avgConsStreetStreamWeek.print();
		KafkaConfigurator.streetKafkaProducer(WEEK_STREET_CONS, avgConsStreetStreamWeek);
	*/

		/**
		 * AVG CONSUMPTION FOR CITY
		 */
		// global average consumption
		AllWindowedStream cityWindowedStreamHour = avgConsStreetStreamHour.timeWindowAll(Time.hours(1));
		SingleOutputStreamOperator avgConsCityStreamHour = cityWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new AvgConsCityFF(), new AvgCityWF()).setParallelism(1);
		avgConsCityStreamHour.print().setParallelism(1);

/*
		AllWindowedStream cityWindowedStreamDay = avgConsStreetStreamDay.timeWindowAll(Time.minutes(3));
		SingleOutputStreamOperator avgConsCityStreamDay = cityWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new AvgConsCityFF(), new AvgCityWF()).setParallelism(1);
		avgConsCityStreamDay.print().setParallelism(1);

		AllWindowedStream cityWindowedStreamWeek = avgConsStreetStreamWeek.timeWindowAll(Time.minutes(5));
		SingleOutputStreamOperator avgConsCityStreamWeek = cityWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new AvgConsCityFF(), new AvgCityWF()).setParallelism(1);
		avgConsCityStreamWeek.print().setParallelism(1);
*/

		/**
		 * 50 MEDIAN
		 */

		WindowedStream LampWindowedStream = lampStream.keyBy(new LampIdKey()).timeWindow(Time.seconds(MEDIAN_WINDOW_SIZE), Time.seconds(MEDIAN_WINDOW_SLIDE));
		SingleOutputStreamOperator lampMedianStream = LampWindowedStream.fold(new Tuple2<>(null, null), new MedianConsLampFF(), new MedianLampWF());
		lampMedianStream.writeAsText("LampMedian");


		AllWindowedStream globalWindowedStream = lampMedianStream.timeWindowAll(Time.seconds(MEDIAN_WINDOW_SIZE),  Time.seconds(MEDIAN_WINDOW_SLIDE));
		SingleOutputStreamOperator globalMedianStream = globalWindowedStream.fold(new Tuple2<>(null, null), new MedianConsLampFF(), new MedianGlobalWF()).setParallelism(1);
		globalMedianStream.writeAsText("GlobalMedian");


		JoinedStreams.WithWindow joinedWindowedStream = lampMedianStream.join(globalMedianStream).where(new LampCityKey()).equalTo(new LampCityKey()).window(TumblingEventTimeWindows.of(Time.seconds(MEDIAN_WINDOW_SLIDE)));
		DataStream joinedMedianStream = joinedWindowedStream.apply(new LocalGlobalMedianJoin());
		joinedMedianStream.writeAsText("JoinedStream");

		WindowedStream groupedStreet = joinedMedianStream.keyBy(new LampAddressKey2()).timeWindow(Time.seconds(2 * MEDIAN_WINDOW_SIZE));
		SingleOutputStreamOperator percentualForStreet = groupedStreet.fold(new Tuple3<>(null, (long) 0,(long) 0), new MedianCountForPercentualFF(), new MedianPercentualWF());
		percentualForStreet.writeAsText("PercentualForStreet");

		SingleOutputStreamOperator filteredPercForStreet = percentualForStreet.keyBy(new StreetIdKey()).filter(new PercentualFilter());


		/**
		 * In questa parte finale ci andrebbe la parte di codice presente al momento
		 * nella classe WriteToKafkaApp che per i test genera il flusso in ingresso a
		 * questo stream all'inizio di questa classe, valutare se scrivere su diversi topic
		 * kafka, al momento scrittura sull'unico topic lampInfo
		 */
		env.execute("Flink Streaming Java API Skeleton");
	}
}