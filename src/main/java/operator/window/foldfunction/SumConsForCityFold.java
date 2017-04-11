package operator.window.foldfunction;

import model.Lamp;
import model.Street;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 08/04/17.
 */
public class SumConsForCityFold implements FoldFunction<Street, Tuple2<Street, Long>> {

    @Override
    public Tuple2<Street, Long> fold(Tuple2<Street, Long> in, Street s) throws Exception {

        if(in.f0 != null)
            return new Tuple2<>(new Street(s.getId(), in.f0.getConsumption() + s.getConsumption(), s.getTimestamp()) , in.f1 + 1);
        else
            return new Tuple2<>(new Street(s.getId(), s.getConsumption(), s.getTimestamp()), (long)1);
    }
}        
