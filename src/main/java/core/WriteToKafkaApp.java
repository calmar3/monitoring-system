package core;

import control.EnvConfigurator;
import model.Lamp;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.util.Time;
import utils.connector.KafkaConfigurator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.exp;


public class WriteToKafkaApp {

    //kafka topic
    private static final String LAMP_DATA_TOPIC = "lamp_data";
    //private static final String LAMP_TOPIC_RANKING= "rank";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();


        /**
         *
         * DATA FOR TEST AVG, WARNING,
         */
/*
		DataStream<Lamp> lampStream = env.addSource(new SourceFunction<Lamp>() {
            @Override
            public void run(SourceContext<Lamp> sourceContext) throws Exception {
                int i = 0;
                long lastSubDate = System.currentTimeMillis();
                while(i < 120) {
                    sourceContext.collect(new Lamp(1, i%2 == 0 ? 0.1 : 0.5 ,"a", lastSubDate, Time.now() + i*1000));
                    sourceContext.collect(new Lamp(2, i%2 == 0 ? 0.1 : 0.5 ,"a", lastSubDate, Time.now() + i*2100));
                    sourceContext.collect(new Lamp(3, i%2 == 0 ? 0.1 : 0.5 ,"a", lastSubDate, Time.now() + i*4300));
                    sourceContext.collect(new Lamp(4, i%2 == 0 ? 0.1 : 0.5 ,"a", lastSubDate, Time.now() + i*8700));
                    sourceContext.collect(new Lamp(5, i%2 == 0 ? 0.9 : 0.9 ,"a", lastSubDate, Time.now() + i*18000));
                    i++;
                }
            }

            @Override
            public void cancel() {

            }
        });
*/


        /**
         * DATA FOR TEST MEDIAN FLOW
         */

        List<Lamp> data = new ArrayList<>();

        for(long i=1; i<=5; i++){
            data.add(new Lamp(1, i*10, "Roma", "via palmiro togliatti", i*1100));
            data.add(new Lamp(2, i*20, "Roma", "via palmiro togliatti", i*1200));
            data.add(new Lamp(3, i*30, "Roma", "via tuscolana", i*1300));
            data.add(new Lamp(4, i*40, "Roma", "via tuscolana", i*1400));
        }

        DataStream<Lamp> lampStream = env.fromCollection(data);


        lampStream.print();

		// emits the stream throught kafka sink
        KafkaConfigurator.lampKafkaProducer(LAMP_DATA_TOPIC, lampStream);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
