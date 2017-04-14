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

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();

        /**
         *
         * DATA FOR TEST AVG, WARNING,
         */
		DataStream<Lamp> lampStream = env.addSource(new SourceFunction<Lamp>() {
            @Override
            public void run(SourceContext<Lamp> sourceContext) throws Exception {
                int i = 0;
                long lastSubDate = System.currentTimeMillis();
                while(i < 100) {
                    sourceContext.collect(new Lamp(1, i%2 == 0 ? 3 : 3 , "Roma", "via palmiro togliatti", lastSubDate, Time.now() + i*10000));
                    sourceContext.collect(new Lamp(2, i%2 == 0 ? 3 : 3 , "Roma", "via palmiro togliatti", lastSubDate, Time.now() + i*10000));
                    sourceContext.collect(new Lamp(3, i%2 == 0 ? 7 : 7 , "Roma", "via tuscolana", lastSubDate, Time.now() + i*10000));
                    sourceContext.collect(new Lamp(4, i%2 == 0 ? 7 : 7 , "Roma", "via tuscolana", lastSubDate, Time.now() + i*10000));
                    //sourceContext.collect(new Lamp(5, i%2 == 0 ? 9 : 9 , "Roma", "via tuscolana", lastSubDate, Time.now() + i*18000));
                    i++;
                }
            }

            @Override
            public void cancel() {

            }
        });


    	// emits the stream throught kafka sink
        KafkaConfigurator.lampKafkaProducer(LAMP_DATA_TOPIC, lampStream);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
