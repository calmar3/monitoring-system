package core;

import control.EnvConfigurator;
import model.Lamp;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.util.Time;
import utils.connector.KafkaConfigurator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class WriteToKafkaApp {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();

		DataStream<Lamp> lampStream = env.addSource(new SourceFunction<Lamp>() {
            @Override
            public void run(SourceContext<Lamp> sourceContext) throws Exception {
                int i = 0;
                while(i < 30) {
                    sourceContext.collect(new Lamp(1,10*(i+1),"a", Time.now() + i*1000));
                    i++;
                }
            }

            @Override
            public void cancel() {

            }
        });

		// emits the stream throught kafka sink
        KafkaConfigurator.getProducer(lampStream);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
