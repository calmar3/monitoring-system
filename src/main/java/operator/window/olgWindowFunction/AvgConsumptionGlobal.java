package operator.window.olgWindowFunction;

import model.Lamp;
import model.Street;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 28/03/17.
 */

//AllWindowFunction<input, output, window>
public class AvgConsumptionGlobal implements AllWindowFunction<Street, Double, Window> {

    @Override
    public void apply(Window window, Iterable<Street> iterable, Collector<Double> collector) throws Exception {

        int n = 0;
        double totalConsumption = 0;
        for (Street tempStreet: iterable) {
            totalConsumption += tempStreet.getConsumption();
            n++;
        }
        collector.collect (new Double(totalConsumption/n));
    }
}
