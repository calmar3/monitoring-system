package core;

import control.EnvConfigurator;
import model.Street;
import operator.filter.ThresholdFilter;
import operator.filter.UpdateGlobalRankFilter;
import operator.key.StreetKey;
import operator.merger.RankMerger;
import operator.ranker.LampRanker;
import operator.time.LampTSExtractor;
import operator.window.cityWindow.CityWindowFunction;
import operator.window.cityWindow.SumConsForCityFold;
import operator.window.lampWindow.LampWindowFunction;
import operator.window.lampWindow.SumConsForLampFold;
import operator.window.streetWindow.StreetWindowFunction;
import operator.window.streetWindow.SumConsForStreetFold;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import utils.connector.KafkaConfigurator;
import operator.filter.FilterByLamp;
import operator.key.LampKey;
import model.Lamp;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.TreeSet;

public class MonitoringApp {

	//Ranking
	private static final int MAX_RANK_SIZE = 3;
	private static final long THRESHOLD = 1000;
	private static final long WINDOW_SIZE = 10000;

	//kafka topic
	private static final String LAMP_TOPIC_CONSUMPTION = "lampInfo";
	private static final String LAMP_TOPIC_RANKING= "rank";


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
		FlinkKafkaConsumer010<Lamp> kafkaConsumer = KafkaConfigurator.getConsumer("lampInfo");

		// assign a timestamp extractor to the consumer
		FlinkKafkaConsumerBase<Lamp> kafkaConsumerTS = kafkaConsumer.assignTimestampsAndWatermarks(new LampTSExtractor());


		// add source
		DataStream<Lamp> lampStream = env.addSource(kafkaConsumerTS);


		// filter data
		DataStream<Lamp> filteredById = lampStream.filter(new FilterByLamp());



/*
		//non creo una classe perchè potrebbe non servirci più se cambiamo il modo in cui calcoliamo la media
		DataStream<Tuple2<Lamp, Long>> addCountToLamp = filteredById.flatMap(new FlatMapFunction<Lamp, Tuple2<Lamp,Long>>() {
			@Override
			public void flatMap(Lamp value, Collector<Tuple2<Lamp,Long>> out){
				out.collect(new Tuple2<>(value, (long) 1));
			}
		});
*/

		/**
		 * INSERT CODE FOR RANKING
		 *
		 */

		// filter data by threshold
		DataStream<Lamp> filteredByThreshold = filteredById.filter(new ThresholdFilter(THRESHOLD));

		//filteredByThreshold.writeAsText("debug1");

		// grouping by lamp id and windowing the stream
		WindowedStream<Lamp, Long , TimeWindow> windowedStream = filteredByThreshold.keyBy(new LampKey()).timeWindow(Time.milliseconds(WINDOW_SIZE));

		// compute partial rank
		SingleOutputStreamOperator<TreeSet<Lamp>> partialRank = windowedStream.apply(new LampRanker(MAX_RANK_SIZE));

		//partialRank.writeAsText("debug2");

		// compute global rank
		DataStream<TreeSet<Lamp>> globalRank = partialRank.flatMap(new RankMerger(MAX_RANK_SIZE)).setParallelism(1);

		// filter not updated global rank
		DataStream<TreeSet<Lamp>> updateGlobalRank = globalRank.filter(new UpdateGlobalRankFilter(MAX_RANK_SIZE)).setParallelism(1);

		// publish result on Kafka topic
		KafkaConfigurator.getProducerRank(updateGlobalRank);





		/**
		 * AVG CONSUMPTION FOR LAMP
		 */

		// average consumption for lamp (hours)
		WindowedStream lampWindowedStreamHour = filteredById.keyBy(new LampKey()).timeWindow(Time.seconds(100));//.trigger(MyTrigger.create());



		//SingleOutputStreamOperator avgConsLampStreamHour = lampWindowedStreamHour.apply(new AvgConsumptionLamp());
		//SingleOutputStreamOperator outputStream = lampWindowedStreamHour.reduce(new AvgReduceFunction(), new LampWindowFunction());
		SingleOutputStreamOperator avgConsLampStreamHour = lampWindowedStreamHour.fold(new Tuple2<>(null, (long) 0), new SumConsForLampFold(), new LampWindowFunction());
		avgConsLampStreamHour.print().setParallelism(1);



		// average consumption for lamp (day)
		WindowedStream lampWindowedStreamDay = avgConsLampStreamHour.keyBy(new LampKey()).timeWindow(Time.minutes(3));
		SingleOutputStreamOperator avgConsLampStreamDay = lampWindowedStreamDay.fold(new Tuple2<>(null, (long) 0), new SumConsForLampFold(), new LampWindowFunction());
		//avgConsLampStreamDay.print().setParallelism(2);


/*
		// average consumption for lamp (week)
		WindowedStream lampWindowedStreamWeek = avgConsLampStreamDay.keyBy(new LampKey()).timeWindow(Time.days(7));
		DataStream<Lamp> avgConsLampStreamWeek = lampWindowedStreamWeek.fold(new Tuple2<>(null, (long) 0), new SumFoldFunction(), new LampWindowFunction());
		avgConsLampStreamDay.print();

*/


		/**
		 * AVG CONSUMPTION FOR STREET
		 */


		// average consumption by street
		WindowedStream streetWindowedStream = avgConsLampStreamHour.keyBy(new StreetKey()).timeWindow(Time.seconds(100));

		DataStream<Street> avgConsStreetStream = streetWindowedStream.fold(new Tuple2<>(null, (long) 0), new SumConsForStreetFold(), new StreetWindowFunction());

		avgConsStreetStream.print().setParallelism(3);


		/**
		 * AVG CONSUMPTION FOR CITY
		 */


		// global average consumption
		AllWindowedStream cityWindowedStream = avgConsStreetStream.timeWindowAll(Time.minutes(3));

		SingleOutputStreamOperator avgConsCityStream = cityWindowedStream.fold(new Tuple2<>(null, (long) 0), new SumConsForCityFold(), new CityWindowFunction()).setParallelism(1);

		avgConsCityStream.print();


		/**
		 * In questa parte finale ci andrebbe la parte di codice presente al momento
		 * nella classe WriteToKafkaApp che per i test genera il flusso in ingresso a
		 * questo stream all'inizio di questa classe, valutare se scrivere su diversi topic
		 * kafka, al momento scrittura sull'unico topic lampInfo
		 */

		env.execute("Flink Streaming Java API Skeleton");
	}
}