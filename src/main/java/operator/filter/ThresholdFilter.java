package operator.filter;

import model.Lamp;
import org.apache.flink.api.common.functions.FilterFunction;

public final class ThresholdFilter implements FilterFunction<Lamp> {
	
	private static final long serialVersionUID = 1L;
	
	private long threshold;
	
	public ThresholdFilter(long threshold) {
		this.threshold = threshold;
	}

	@Override
	public boolean filter(Lamp lamp) throws Exception {
		if(lamp.getResidualLifeTime() < threshold)
			return false;
		
		else
			return true;
	}
	
}
