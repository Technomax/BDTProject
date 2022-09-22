package cs523.SparkConsumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WeatherConsumer {
	private KafkaConsumer<String, String> consumer;
	private WeatherResources resources;
	private String lastKey="";
	public WeatherConsumer(WeatherResources resources)
	{
		this.resources=resources;
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:"+resources.PORT);
		properties.put("group.id", "group01");
		properties.put("enable.auto.commit", "true");
		properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
	    properties.put("auto.offset.reset", "earliest");
	    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    this.consumer = new KafkaConsumer<String, String>(properties);
	    this.consumer.subscribe(Arrays.asList(resources.TOPIC));
	}
	
	public KafkaConsumer<String, String> getConsumer(){
		return this.consumer;
	}
	
//	public void Wait(MyPredict predictable,MyConsumerCallback callback)
//	{
//		while (predictable.check()) 
//	    {
//	        @SuppressWarnings("deprecation")
//	        ConsumerRecords<String, String> records = this.consumer.poll(100);
//	        for (ConsumerRecord<String, String> record : records)
//	        {
//	        	callback.consume(record.key(),record.value(),this.lastKey.compareTo(record.key().toLowerCase())!=0);
//	        	this.lastKey=record.key().toLowerCase();
//	        }
//	    }
//	}
//	
//	@FunctionalInterface
//	public interface MyPredict 
//	{
//	    boolean check();
//	}
//	
//	@FunctionalInterface
//	public interface MyConsumerCallback
//	{
//	    void consume(String key, String val,boolean new_key);
//	}
//	
}
