package utils.serialization;

import model.Street;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.calcite.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.calcite.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

/**
 * Created by maurizio on 19/04/17.
 */
public class CitySchema implements SerializationSchema<Double>{

    public ObjectMapper mapper;

    @Override
    public byte[] serialize(Double element) {

        this.mapper = new ObjectMapper();

        String jsonInString = new String("");
        try {
            jsonInString = mapper.writeValueAsString(element);
            return jsonInString.getBytes();

        } catch (JsonProcessingException e) {

            return null;

        }
    }
}
