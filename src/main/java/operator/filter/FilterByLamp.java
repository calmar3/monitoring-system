package operator.filter;

import utils.structure.LampsAvl;
import model.Lamp;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by marco on 25/03/17.
 */
public final class FilterByLamp implements FilterFunction<Lamp> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(Lamp lamp) throws Exception {

        /**
         * discard invalid tuple
         */
        if (lamp == null)
            return false;

        /**
         * discard tuple not in avl
         */
        /*if (LampsAvl.getInstance().get(new Long(lamp.getId())) == null)
            return false;
        else {
            //lamp.toString();
            return true;
        }*/

        return true;

    }
}