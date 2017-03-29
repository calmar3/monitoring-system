package operator.time;

import model.Lamp;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

/**
 * Created by maurizio on 29/03/17.
 */
public class AscendingTimestamper extends AscendingTimestampExtractor {


    @Override
    public long extractAscendingTimestamp(Object o) {
        return 0;
    }
}
