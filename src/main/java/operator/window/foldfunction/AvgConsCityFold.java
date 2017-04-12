package operator.window.foldfunction;

import model.Street;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 11/04/17.
 */
public class AvgConsCityFold implements FoldFunction<Street, Tuple2<Street, Long>> {

    @Override
    public Tuple2<Street, Long> fold(Tuple2<Street, Long> in, Street s) throws Exception {

        if(in.f0 != null) {
            s.setConsumption(in.f0.getConsumption() + (s.getConsumption() - in.f0.getConsumption()) / (in.f1 + 1));
            return new Tuple2<>(s, in.f1 + 1);
        }
        else
            return new Tuple2<>(s, (long)1);
    }
}
