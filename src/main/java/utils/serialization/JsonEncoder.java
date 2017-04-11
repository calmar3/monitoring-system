package utils.serialization;


import model.Lamp;
import org.apache.calcite.avatica.org.apache.commons.codec.Encoder;
import org.apache.flink.shaded.calcite.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.calcite.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by maurizio on 10/04/17.
 */
public class JsonEncoder {

    public static String serialize(Object o) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

}
