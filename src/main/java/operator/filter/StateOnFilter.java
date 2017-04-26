package operator.filter;

import model.Lamp;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 14/04/17.
 */
public class StateOnFilter implements FilterFunction<Lamp> {

    @Override
    public boolean filter(Lamp lamp) throws Exception{

        if(!lamp.isStateOn() && lamp.getConsumption() > 0)
            return true;
        else
            return false;
    }
}
