package operator.key;

import model.Lamp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 11/04/17.
 */
public class LampIdKey2 implements KeySelector<Tuple2<Lamp, Double>, Long> {

    @Override
    public Long getKey(Tuple2<Lamp, Double> t) throws Exception {
        return t.f0.getLampId();
    }
}