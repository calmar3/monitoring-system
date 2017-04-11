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
public class CityWindowFunction implements AllWindowFunction<Tuple2<Street, Long>, Tuple2<Double, Long>, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Street, Long>> input, Collector<Tuple2<Double, Long>> out) throws Exception {

        Tuple2<Street, Long> avgCons = input.iterator().next();

        out.collect(new Tuple2<>(avgCons.f0.getConsumption(), avgCons.f0.getTimestamp()));
    }
}
