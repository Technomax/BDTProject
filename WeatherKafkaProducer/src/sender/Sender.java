package sender;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.awt.List;
import java.util.Properties;

public class Sender {
	private final static String SERVER ="localhost:9092";
    private final static String TOPIC = "weather-topic";
    
    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put("bootstrap.servers", SERVER);
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }
    
    public static void sendWeather(String id, String text) throws Exception {
        final Producer<String, String> producer = createProducer();
        final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, id, text);
        producer.send(record).get();       
    }
}
