package operator.filter;

import model.Lamp;
import model.Street;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * Created by maurizio on 12/04/17.
 */
public class PercentualFilter implements FilterFunction<Tuple2<Street, Double>> {

    private static final long serialVersionUID = 1L;

    private Double lastPercentual = null;

    public void PercentualFilter(Double percentual){this.lastPercentual = percentual;}

    @Override
    public boolean filter(Tuple2<Street, Double> percentualOut) throws Exception {
        if(lastPercentual != null) {
            if(percentualOut.f1 == this.lastPercentual)
                return false;
            else
                return true;
        }
        else {
            this.lastPercentual = percentualOut.f1;
            return true;
        }
    }
}

