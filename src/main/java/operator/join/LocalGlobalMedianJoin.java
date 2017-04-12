package operator.join;

import model.Lamp;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class LocalGlobalMedianJoin implements  JoinFunction<Lamp, Lamp,Tuple2<Lamp, Boolean>>{

	private static final long serialVersionUID = 1L;

	@Override
	public Tuple2<Lamp, Boolean> join(Lamp lamp, Lamp global) throws Exception {

		if(lamp.getConsumption() > global.getConsumption())
			return new Tuple2<>(lamp, true);
		else
			return new Tuple2<>(lamp, false);

	}		
}