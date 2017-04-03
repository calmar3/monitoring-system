package operator.time;

import model.Lamp;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by maurizio on 30/03/17.
 *
 * Assigns timestamps to Lamp records.
 * Watermarks are a fixed time interval behind the max timestamp and are periodically emitted.
 */
public class LampTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<Lamp> {

    private static final long MAX_EVENT_DELAY = 10 ;

    public LampTSExtractor() {
        super(Time.seconds(MAX_EVENT_DELAY));
    }

    @Override
    public long extractTimestamp(Lamp lamp) {
            //System.out.println(lamp.getTimestamp());
            return lamp.getTimestamp();
    }
}
