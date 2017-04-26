package operator.window.windowfunction;

import model.Street;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 06/04/17.
 */

//WindowFunction<input, output, key, window>
public class AvgStreetWF implements WindowFunction<Tuple2<Street, Long>, Street, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<Street, Long>> input, Collector<Street> out) throws Exception {

        Tuple2<Street, Long> avgConsStreet = input.iterator().next();
        //System.out.println("AvgLampWF result " + avgLamp.getConsumption() + " Timestamp " + avgLamp.getTimestamp());
        out.collect(avgConsStreet.f0.clone());
    }
}
