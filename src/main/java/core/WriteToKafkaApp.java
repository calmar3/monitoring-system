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
    private static final String LAMP_TOPIC_CONSUMPTION = "lampInfo";
    private static final String LAMP_TOPIC_RANKING= "rank";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();

        /*
		DataStream<Lamp> lampStream = env.addSource(new SourceFunction<Lamp>() {
            @Override
            public void run(SourceContext<Lamp> sourceContext) throws Exception {
                int i = 0;
                while(i < 120) {
                    sourceContext.collect(new Lamp(1, i%2 == 0 ? 4 : 6 ,"a", Time.now() + i*1000));
                    i++;
                }
            }

            @Override
            public void cancel() {

            }
        });
*/


        DataStream<Lamp> lampStream = env.addSource(new SourceFunction<Lamp>() {
            @Override
            public void run(SourceContext<Lamp> sourceContext) throws Exception {
                int i = 1;
                long lastSubDate = System.currentTimeMillis();
                while(i <= 25) {
                    sourceContext.collect(new Lamp(1, lastSubDate + i*1000, lastSubDate));
                    sourceContext.collect(new Lamp(2, lastSubDate + i*2100, lastSubDate));
                    sourceContext.collect(new Lamp(3, lastSubDate + i*4300, lastSubDate));
                    sourceContext.collect(new Lamp(4, lastSubDate + i*8700, lastSubDate));
                    sourceContext.collect(new Lamp(5, lastSubDate + i*18000, lastSubDate));
                    i++;
                }
            }

            @Override
            public void cancel() {

            }
        });

		lampStream.print();

		// emits the stream throught kafka sink
        KafkaConfigurator.getProducerCons(lampStream);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
