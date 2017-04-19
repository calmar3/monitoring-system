package utils.serialization;

import model.Street;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.calcite.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.calcite.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

/**
 * Created by maurizio on 11/04/17.
 */
public class StreetSchema implements DeserializationSchema<Street>, SerializationSchema<Street> {

    public ObjectMapper mapper;

    @Override
    public byte[] serialize(Street element) {

        this.mapper = new ObjectMapper();

        String jsonInString = new String("");
        try {

            jsonInString = mapper.writeValueAsString(element);
            return jsonInString.getBytes();

        } catch (JsonProcessingException e) {

            return null;

        }

    }

    @Override
    public Street deserialize(byte[] message) {
        return null;
    }

    @Override
    public boolean isEndOfStream(Street nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Street> getProducedType() {
        return TypeExtractor.getForClass(Street.class);
    }
}