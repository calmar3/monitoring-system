package operator.key;

import model.Lamp;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;


public class LampAddressKey2 implements KeySelector<Tuple2<Lamp, Boolean>, String> {

	private static final long serialVersionUID = 1L;

	@Override
    public String getKey(Tuple2<Lamp, Boolean> lamp) throws Exception {
        return lamp.f0.getAddress();
    }
}