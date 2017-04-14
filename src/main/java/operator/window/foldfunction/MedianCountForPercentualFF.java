package operator.window.foldfunction;

import model.Lamp;
import model.Street;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class MedianCountForPercentualFF implements FoldFunction<Tuple2<Lamp, Boolean>, Tuple3<Street, Long, Long>> {


	@Override
	public Tuple3<Street, Long, Long> fold(Tuple3<Street, Long, Long> accumulator, Tuple2<Lamp, Boolean> value) throws Exception {

		if(accumulator.f0 != null)
			return new Tuple3<>(new Street(value.f0.getAddress(), value.f0.getTimestamp()), accumulator.f1+1, value.f1 == true ? (accumulator.f2 + 1) : accumulator.f2 );
		else
			return new Tuple3<>(new Street(value.f0.getAddress(), value.f0.getTimestamp()),(long) 1, value.f1 == true ? 1L : 0);
	}
}