package operator.window.olgWindowFunction;

import model.Lamp;
import model.Street;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 28/03/17.
 */

//WindowFunction<input, output, key, window>
public class AvgConsumptionStreet implements WindowFunction<Lamp, Street, String, Window> {

    @Override
    public void apply (String key, Window window, Iterable<Lamp> input, Collector<Street> out) throws Exception {

        double totalConsumption = 0;
        int n = 0;
        for (Lamp tempLamp: input) {
            totalConsumption += tempLamp.getConsumption();
            n++;
        }
        out.collect(new Street(key, totalConsumption/n, 0));
    }
}
