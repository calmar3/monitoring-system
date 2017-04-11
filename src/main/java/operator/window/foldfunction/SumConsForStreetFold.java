package operator.window.foldfunction;

import model.Lamp;
import model.Street;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 08/04/17.
 */
public class SumConsForStreetFold implements FoldFunction<Lamp, Tuple2<Street, Long>> {

    @Override
    public Tuple2<Street, Long> fold(Tuple2<Street, Long> in, Lamp l) throws Exception {

        if(in.f0 != null)
            return new Tuple2<>(new Street(l.getAddress(), in.f0.getConsumption() + l.getConsumption(), l.getTimestamp()) , in.f1 + 1);
        else
            return new Tuple2<>(new Street(l.getAddress(), l.getConsumption(), l.getTimestamp()), (long)1);
    }
}        
