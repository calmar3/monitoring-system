package core;

import control.EnvConfigurator;
import model.Lamp;
import org.apache.hadoop.util.Time;
import utils.connector.KafkaConfigurator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class WriteToKafkaApp {

    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String LAMP_TOPIC = "lampInfo";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = EnvConfigurator.setupExecutionEnvironment();


        // generate data
		List<Lamp> data = new ArrayList<>();
        data.add(new Lamp(1,10,"a", Time.now()));
        data.add(new Lamp(1,20,"a", Time.now()));
        data.add(new Lamp(1,20,"a", Time.now()));
        data.add(new Lamp(3,12.1,"b", Time.now()));
        data.add(new Lamp(1,10,"a", Time.now()));
        data.add(new Lamp(2,1,"b", Time.now()));
        data.add(new Lamp(2,9,"b", Time.now()));
        data.add(new Lamp(1,20,"a", Time.now() + 10000));

        // Final watermark, see https://issues.apache.org/jira/browse/FLINK-3121
        data.add(new Lamp(0, 0,"", Long.MAX_VALUE));

		DataStream<Lamp> lampStream = env.fromCollection(data);

		// emits the stream throught kafka sink
        KafkaConfigurator.getProducer(lampStream);

        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
