package org.apache.flink.quickstart;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Created by maurizio on 21/03/17.
 * 
 * Implements a SerializationSchema and DeserializationSchema for Lamp for Kafka data sources and sinks.
 */
public class LampSchema implements DeserializationSchema<Lamp>, SerializationSchema<Lamp> {

    @Override
    public byte[] serialize(Lamp element) {
        return element.toString().getBytes();
    }

    @Override
    public Lamp deserialize(byte[] message) {
        return Lamp.fromString(new String(message));
    }

    @Override
    public boolean isEndOfStream(Lamp nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Lamp> getProducedType() {
        return TypeExtractor.getForClass(Lamp.class);
    }
}