package operator.window.reduce;

import model.Lamp;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Created by maurizio on 31/03/17.
 */
public class AvgReduceFunction implements ReduceFunction<Tuple2<Lamp, Long>> {

    @Override
    public Tuple2<Lamp, Long> reduce(Tuple2<Lamp, Long> l1, Tuple2<Lamp, Long> l2) throws Exception {
        System.out.println("L1 " + l1.f0.getConsumption() + " \tTimeStamp " + l1.f0.getTimestamp() + " \t" + l1.f1 );
        System.out.println("L2 " + l2.f0.getConsumption() + " \t\tTimeStamp " + l2.f0.getTimestamp() + " \t" + l2.f1);


        //l1.f0.setConsumption(l1.f0.getConsumption() + l2.f0.getConsumption());
        //l1.f0.setTimestamp(l2.f0.getTimestamp());

      //  l1.setFields(l1.f0, l1.f1 + 1);

        System.out.println("Sum " + (l1.f0.getConsumption() + l2.f0.getConsumption()) + " \tTimeStamp " + l2.f0.getTimestamp()  + " \t" + l1.f1 + "\n");


        return new Tuple2<>(new Lamp(l1.f0.getLampId(), l1.f0.getConsumption() + l2.f0.getConsumption(), l1.f0.getAddress(), l2.f0.getTimestamp()), l1.f1 + 1 );
    }
}