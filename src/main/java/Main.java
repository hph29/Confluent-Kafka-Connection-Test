/**
 * Created by hang on 02/11/17.
 */

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class Main {

    static class Producer implements Runnable {

        public void run() {
            produce();
        }

        void produce() {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
            props.put("schema.registry.url", "http://localhost:8081");
            KafkaProducer producer = new KafkaProducer(props);

            String key = "key1";
            String userSchema = "{\"type\":\"record\"," +
                    "\"name\":\"myrecord\"," +
                    "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(userSchema);
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("f1", "value1");

            ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("topic1", key, avroRecord);
            try {
                for (int i = 0; i < 10; i++){
                    System.out.println("Sending message: " + (i+1));
                    producer.send(record);
                    Thread.sleep(1000L);
                }

            } catch (SerializationException e) {
                // may need to do something with it
            } catch (InterruptedException e) {

            }
        }
    }


    public static void main(String[] args) {

        Producer p = new Main.Producer();

        Thread producerThread = new Thread(p);

        producerThread.start();

        consume();
    }
    public static void consume(){


        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "group2");
        props.put("schema.registry.url", "http://localhost:8081");

        String topic = "topic1";
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        VerifiableProperties vProps = new VerifiableProperties(props);
        KafkaAvroDecoder keyDecoder = new KafkaAvroDecoder(vProps);
        KafkaAvroDecoder valueDecoder = new KafkaAvroDecoder(vProps);

        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<Object, Object>>> consumerMap = consumer.createMessageStreams(
                topicCountMap, keyDecoder, valueDecoder);
        KafkaStream stream = consumerMap.get(topic).get(0);
        ConsumerIterator it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();

            try {
                String key = (String) messageAndMetadata.key();
                IndexedRecord value = (IndexedRecord) messageAndMetadata.message();
                System.out.println("Getting message: " + value.get(0));
            } catch(SerializationException e) {
                // may need to do something with it
            }
        }
    }

}
