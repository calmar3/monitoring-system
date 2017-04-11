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

        String jsonInString = new String(message);
        this.mapper = new ObjectMapper();
        //Street lamp = new Street();
        try {

            Street lamp = this.mapper.readValue(jsonInString, Street.class);
            return lamp;

        }  catch (IOException e) {

            /**
             * .readValue catch exception over any kind of data (double, int, objects ecc ecc)
             * return null value to discard invalid tuple
             */
            return null;

        }

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