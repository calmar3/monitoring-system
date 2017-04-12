package operator.key;

import model.Street;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 12/04/17.
 */
public class StreetIdKey implements KeySelector<Tuple2<Street, Double>, String> {

    @Override
    public String getKey(Tuple2<Street, Double> percentualStreet) throws Exception {
        return percentualStreet.f0.getId();
    }
}
