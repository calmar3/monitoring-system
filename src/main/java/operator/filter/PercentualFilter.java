package operator.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.structure.HashMapMedian;

import java.util.HashMap;


/**
 * Created by maurizio on 12/04/17.
 */
@SuppressWarnings("ALL")
public class PercentualFilter implements FilterFunction<Tuple2<String, Double>> {

    @Override
    public boolean filter(Tuple2<String, Double> medianTuple) throws Exception {
        HashMap<String, Double> medianMap = HashMapMedian.getInstance();

        Double median = medianMap.get(medianTuple.f0);

        if(median == null){
            medianMap.put(medianTuple.f0, medianTuple.f1);
            return true;
        }
        else if(Double.compare(median, medianTuple.f1) == 0)
            return false;
        else {
            medianMap.replace(medianTuple.f0, medianTuple.f1);
            return true;
        }
    }
}

