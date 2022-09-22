package cs523.SparkConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class App {
	static String lastKey = "";
	static List<String> list = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		WeatherSpark spark = new WeatherSpark();
		WeatherResources resources = new WeatherResources();
		KafkaConsumer<String, String> consumer = new WeatherConsumer(resources).getConsumer();
		while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);         
            consumerRecords.forEach(record -> {
            	System.out.println(record.key());
            	System.out.println(record.value());
            	try {
					spark.Process(record.key(), Arrays.asList(record.value().split("\\n")));
				} catch (Exception e) {
					e.printStackTrace();
				}
            });
        }
	}
}
