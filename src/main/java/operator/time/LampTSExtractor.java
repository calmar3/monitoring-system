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
 *
 *
 *
 *
 * Another example of periodic watermark generation is when the watermark lags behind the maximum (event-time) timestamp seen
 * in the stream by a fixed amount of time. This case covers scenarios where the maximum lateness that can be encountered in a
 * stream is known in advance, e.g. when creating a custom source containing elements with timestamps spread within a fixed
 * period of time for testing. For these cases, Flink provides the BoundedOutOfOrdernessTimestampExtractor which takes as an
 * argument the maxOutOfOrderness, i.e. the maximum amount of time an element is allowed to be late before being ignored when
 * computing the final result for the given window. Lateness corresponds to the result of t - t_w, where t is the (event-time)
 * timestamp of an element, and t_w that of the previous watermark. If lateness > 0 then the element is considered late and is,
 * by default, ignored when computing the result of the job for its corresponding window. See the documentation about allowed
 * lateness for more information about working with late elements.
 */
public class LampTSExtractor extends BoundedOutOfOrdernessTimestampExtractor<Lamp> {

    private static final long MAX_EVENT_DELAY = 1;

    public LampTSExtractor() {
        super(Time.seconds(MAX_EVENT_DELAY));
    }

    @Override
    public long extractTimestamp(Lamp lamp) {
        //System.out.println(lamp.getTimestamp());
        return lamp.getTimestamp();
    }
}
