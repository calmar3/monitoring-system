package core;

import control.EnvConfigurator;
import operator.key.StreetKey;
import operator.time.LampTSExtractor;
import operator.window.AvgConsumptionGlobal;
import operator.window.AvgConsumptionLamp;
import operator.window.AvgConsumptionStreet;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
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
import utils.structure.LampsAvl;

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

		// assign a timestamp extractor to the consumer
		FlinkKafkaConsumerBase<Lamp> kafkaConsumerTS = kafkaConsumer.assignTimestampsAndWatermarks(new LampTSExtractor());

		// add source
		DataStream<Lamp> lampStream = env.addSource(kafkaConsumerTS);

		// filter data
		DataStream<Lamp> filteredById = lampStream.filter(new FilterByLamp()).setParallelism(1);




		// average consumption by lampId
		WindowedStream windowedStream = filteredById.keyBy(new LampKey()).timeWindow(Time.seconds(3));

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