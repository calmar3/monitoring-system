package operator.window.foldfunction;

import model.Lamp;
import model.Street;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.connector.KafkaConfigurator;

/**
 * Created by maurizio on 10/04/17.
 */
public class AvgConsStreetWarnFold implements FoldFunction<Lamp, Tuple2<Street, Long>> {

    String warnTopic = "";

    public AvgConsStreetWarnFold(String warnTopic) {
        this.warnTopic = warnTopic;
    }

    @Override
    public Tuple2<Street, Long> fold(Tuple2<Street, Long> in, Lamp l) throws Exception {

        if(in.f0 != null) {
            double delta = l.getConsumption() - in.f0.getConsumption();
            if(delta > 0.5)
                KafkaConfigurator.warningKafkaProducer(this.warnTopic, String.valueOf(l.getLampId()), l);

            return new Tuple2<>(new Street(l.getAddress(), in.f0.getConsumption() + delta/(in.f1 + 1), l.getTimestamp()), in.f1 + 1);
        }
        else
            return new Tuple2<>(new Street(l.getAddress(), l.getConsumption(), l.getTimestamp()), (long)1);
    }
}
