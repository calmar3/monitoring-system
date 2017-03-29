package operator.key;

import org.apache.flink.api.java.functions.KeySelector;
import model.Lamp;

/**
 * Created by marco on 27/03/17.
 */
public class StreetKey implements KeySelector<Lamp, String> {

    @Override
    public String getKey(Lamp lamp) throws Exception {
        return lamp.getAddress();
    }
}

