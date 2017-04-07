package operator.window.olgWindowFunction;

import model.Lamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 28/03/17.
 */

//WindowFunction<input, output, key, window>
public class AvgConsumptionLamp implements WindowFunction<Lamp, Lamp, Long, TimeWindow> {

    @Override
    public void apply (Long key, TimeWindow window, Iterable<Lamp> input, Collector<Lamp> out) throws Exception {

        double totalConsumption = 0;
        int n = 0;
        Lamp lamp = null;
        for (Lamp tempLamp: input) {
            if (lamp == null)
                lamp = tempLamp;
            totalConsumption += tempLamp.getConsumption();
            System.out.println(tempLamp.toString());
            n++;
        }
        out.collect(new Lamp(key, totalConsumption/n,lamp.getAddress(),0l));
    }
}