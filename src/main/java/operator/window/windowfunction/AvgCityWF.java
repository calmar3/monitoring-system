package operator.window.windowfunction;

import model.Lamp;
import model.Street;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 06/04/17.
 */
public class AvgCityWF implements AllWindowFunction<Tuple2<Street, Long>, Double, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Street, Long>> input, Collector<Double> out) throws Exception {

        Tuple2<Street, Long> avgCons = input.iterator().next();

        out.collect(new Double(avgCons.f0.getConsumption()));
    }
}
