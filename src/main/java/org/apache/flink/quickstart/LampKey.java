package org.apache.flink.quickstart;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Created by maurizio on 23/03/17.
 */
public class LampKey implements KeySelector<Lamp, Long> {

    @Override
    public Long getKey(Lamp lamp) throws Exception {
        return lamp.getId();
    }
}
