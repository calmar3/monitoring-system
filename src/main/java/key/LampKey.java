package key;

import org.apache.flink.api.java.functions.KeySelector;
import model.Lamp;

/**
 * Created by maurizio on 23/03/17.
 */
public class LampKey implements KeySelector<Lamp, Long> {

    @Override
    public Long getKey(Lamp lamp) throws Exception {
        return lamp.getId();
    }
}
