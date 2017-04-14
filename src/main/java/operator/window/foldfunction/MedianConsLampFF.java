package operator.window.foldfunction;

import model.Lamp;
import utils.median.TDigestMedian;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Created by maurizio on 12/04/17.
 */
public class MedianConsLampFF implements FoldFunction<Lamp, Tuple2<TDigestMedian, Lamp>> {

    @Override
    public Tuple2<TDigestMedian, Lamp> fold(Tuple2<TDigestMedian, Lamp> accumulator, Lamp l) throws Exception {
        if(accumulator.f0 != null) {
            TDigestMedian median = new TDigestMedian();
            median.setTotalDigest(accumulator.f0.getTotalDigest());
            median.addDigest(l.getConsumption());
            return new Tuple2<>(median, l);
        }
        else {
            TDigestMedian median = new TDigestMedian();
            median.addDigest(l.getConsumption());
            return new Tuple2<>(median, l);
        }
    }

}
