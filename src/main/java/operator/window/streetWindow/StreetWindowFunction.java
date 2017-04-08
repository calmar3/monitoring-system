package operator.window.streetWindow;

import model.Street;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 06/04/17.
 */

//WindowFunction<input, output, key, window>
public class StreetWindowFunction implements WindowFunction<Tuple2<Street, Long>, Street, String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<Street, Long>> input, Collector<Street> out) throws Exception {

        Tuple2<Street, Long> totConsLampInStreet = input.iterator().next();

        //System.out.println("LampWindowFunction result " + avgLamp.getConsumption() + " Timestamp " + avgLamp.getTimestamp());
        out.collect(new Street(key, totConsLampInStreet.f0.getConsumption() / totConsLampInStreet.f1, totConsLampInStreet.f0.getTimestamp()));
    }
}
