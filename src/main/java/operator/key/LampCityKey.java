package operator.key;

import model.Lamp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class LampCityKey implements KeySelector<Lamp, String> {

	private static final long serialVersionUID = 1L;

	@Override
    public String getKey(Lamp lamp) throws Exception {
        return lamp.getCity();
    }
}