package operator.window;

import model.Lamp;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Created by maurizio on 28/03/17.
 */

//WindowFunction<input, output, key, window>
public class LampWindowFunction implements WindowFunction<Lamp, Lamp, Long, TimeWindow> {

    @Override
    public void apply (Long key, TimeWindow window, Iterable<Lamp> input, Collector<Lamp> out) throws Exception {

        Lamp lampAvg = input.iterator().next();
        //System.out.println("WindowFunction, lampAvgconsumption = " + lampAvg.getConsumption());
        out.collect(lampAvg);
    }
}
