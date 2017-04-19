package utils.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.Lamp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

/**
 * Created by maurizio on 21/03/17.
 * 
 * Implements a SerializationSchema and DeserializationSchema for Lamp for Kafka data sources and sinks.
 */
public class LampSchema implements DeserializationSchema<Lamp>, SerializationSchema<Lamp> {

    public ObjectMapper mapper;

    @Override
    public byte[] serialize(Lamp element) {

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
    public Lamp deserialize(byte[] message) {

        String jsonInString = new String(message);
        //System.out.println("\n\n\n\n\n\n\n\n" + jsonInString + "\n\n\n\n\n\n\n\n");
        this.mapper = new ObjectMapper();

        try {

            Lamp lamp = this.mapper.readValue(jsonInString, Lamp.class);
            return lamp;

        }  catch (IOException e) {

            /**
             * .readValue catch exception over any kind of data (double, int, objects ecc ecc)
             * return null value to discard invalid tuple
             */
            System.out.println("\n\n\n\n\n\n\n\n");
            e.printStackTrace();
            System.out.println("\n\n\n\n\n\n\n\n");
            return null;

        }

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