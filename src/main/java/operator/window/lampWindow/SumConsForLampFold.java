package operator.window.lampWindow;

import model.Lamp;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 08/04/17.
 */
public class SumConsForLampFold implements FoldFunction<Lamp, Tuple2<Lamp, Long>> {

    @Override
    public Tuple2<Lamp, Long> fold(Tuple2<Lamp, Long> in, Lamp l) throws Exception {
        if(in.f0 != null)
            return new Tuple2<>(new Lamp(l.getLampId(), in.f0.getConsumption() + l.getConsumption(), l.getAddress(), l.getTimestamp()), in.f1 + 1);
        else
            return new Tuple2<>(l, (long)1);
    }

}