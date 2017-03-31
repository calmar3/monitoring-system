package operator.window;

import model.Lamp;
import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Created by maurizio on 31/03/17.
 */
public class AvgReduceFunction implements ReduceFunction<Lamp> {

    private long N = 1;

    public Lamp reduce(Lamp l1, Lamp l2) {

        N++;
        //System.out.println("Reduce N=" + N + "  consumptionl1=" + l1.getConsumption() + "\n");
        //System.out.println("Reduce N=" + N + "  consumptionl2=" + l2.getConsumption() + "\n");
        l1.setConsumption(((l1.getConsumption() * (N-1) + l2.getConsumption()) / N));
        //System.out.println("Reduce N=" + N + "  consumptionl1Calculator=" + l1.getConsumption() + "\n");
        return l1;
    }
}

