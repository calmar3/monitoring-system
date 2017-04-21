package operator.window.windowfunction;

import model.Street;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


//WindowFunction<input, output, key, window>
public class MedianPercentualWF implements WindowFunction<Tuple3<Street, Long, Long>, Tuple2<String, Double>, String, TimeWindow> {

	@Override
	public void apply(String key, TimeWindow window, Iterable<Tuple3<Street, Long, Long>> input, Collector<Tuple2<String, Double>> out) throws Exception {
		 
		Tuple3<Street, Long, Long> totalPercentual = input.iterator().next();
		
		double percentual = (double) totalPercentual.f2/totalPercentual.f1;
	    
		out.collect(new Tuple2<>(totalPercentual.f0.getId(), percentual));
	}
}

