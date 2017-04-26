package control;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.Lamp;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by maurizio on 28/03/17.
 */
public class EnvConfigurator {

    public static final StreamExecutionEnvironment setupExecutionEnvironment(/*AppConfiguration config*/) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // generate a Watermark every second
        env.getConfig().setAutoWatermarkInterval(1000);

        return env;
    }
}
