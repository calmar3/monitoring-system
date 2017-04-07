package control;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by maurizio on 28/03/17.
 */
public class EnvConfigurator {

    public static final StreamExecutionEnvironment setupExecutionEnvironment(/*AppConfiguration config*/) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // generate a Watermark every second
        env.getConfig().setAutoWatermarkInterval(100);

        return env;
    }

}
