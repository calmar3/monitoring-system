package utils.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.Lamp;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;
import java.util.TreeSet;

/*valerio*/

public class LampRankSchema implements DeserializationSchema<TreeSet<Lamp>>, SerializationSchema<TreeSet<Lamp>> {

	private static final long serialVersionUID = 1L;
	
	public ObjectMapper mapper;

    @Override
    public byte[] serialize(TreeSet<Lamp> element) {

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
    public boolean isEndOfStream(TreeSet<Lamp> nextElement) {
        return false;
    }

	@Override
	public TypeInformation<TreeSet<Lamp>> getProducedType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TreeSet<Lamp> deserialize(byte[] message) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}