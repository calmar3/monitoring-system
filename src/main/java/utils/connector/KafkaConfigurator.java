package utils.connector;

import model.Lamp;

import model.Street;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.serialization.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;
import java.util.TreeSet;



/**
 * Created by maurizio on 28/03/17.
 */
public class KafkaConfigurator {

    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";

    public static final FlinkKafkaConsumer010<Lamp> kafkaConsumer(String topic) {

        // configure the Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("group.id", "myGroup");

        // always read the Kafka topic from the start
        kafkaProps.setProperty("auto.offset.reset", "earliest");

        // create a Kafka consumer
        FlinkKafkaConsumer010<Lamp> consumer = new FlinkKafkaConsumer010<>(
                topic,          //kafka topic
                new LampSchema(),   //deserialization schema
                kafkaProps);        //consumer configuration

        return consumer;
    }


    public static final void lampKafkaProducer(String topic, DataStream<Lamp> lampStream) {

        //write data to a Kafka sink
        lampStream.addSink(new FlinkKafkaProducer010<>(
                LOCAL_KAFKA_BROKER,
                topic,
                new LampSchema()
        ));


    }

    public static final void streetKafkaProducer(String topic, DataStream<Street> streetStream) {

        //write data to a Kafka sink
        streetStream.addSink(new FlinkKafkaProducer010<>(
                LOCAL_KAFKA_BROKER,
                topic,
                new StreetSchema()
        ));
    }

    public static final void cityKafkaProducer(String topic, DataStream<Double> streetStream) {

        //write data to a Kafka sink
        streetStream.addSink(new FlinkKafkaProducer010<>(
                LOCAL_KAFKA_BROKER,
                topic,
                new CitySchema()
        ));
    }

    public static final void rankKafkaProducer(String topic, DataStream<TreeSet<Lamp>> lampRank) {

        //write data to a Kafka sink
        lampRank.addSink(new FlinkKafkaProducer010<>(
                LOCAL_KAFKA_BROKER,
                topic,
                new LampRankSchema()
        )).setParallelism(1);
    }

    public static final void medianKafkaProducer(String topic, DataStream<Tuple2<String, Double>> lampStream) {

        //write data to a Kafka sink
        lampStream.addSink(new FlinkKafkaProducer010<>(
                LOCAL_KAFKA_BROKER,
                topic,
                new MedianSchema()
        ));
    }

    public static final void warningKafkaProducer(String topic, String key, Lamp l) {
        Properties props = new Properties();
        props.put("bootstrap.servers", LOCAL_KAFKA_BROKER);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topic, key, JsonEncoder.serialize(l)));

        producer.close();
    }
}
