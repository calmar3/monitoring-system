package operator.window.foldfunction;

import model.Lamp;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 11/04/17.
 */
public class AvgConsLampFF implements FoldFunction<Lamp, Tuple2<Lamp, Long>> {

    @Override
    public Tuple2<Lamp, Long> fold(Tuple2<Lamp, Long> in, Lamp l) throws Exception {

        if(in.f0 != null) {
            l.setConsumption(in.f0.getConsumption() + (l.getConsumption() - in.f0.getConsumption()) / (in.f1 + 1));
            return new Tuple2<>(l, in.f1 + 1);
        }
        else
            return new Tuple2<>(l, (long)1);
    }
}