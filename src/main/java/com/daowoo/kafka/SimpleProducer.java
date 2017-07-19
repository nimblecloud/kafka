package com.daowoo.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;

public class SimpleProducer {
	
	static Producer<String, String> getProducer(String server) {
		Properties props = new Properties();
		props.put("bootstrap.servers", server);
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("buffer.memory", 33554432);
		props.put("client.id", "test_program");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		props.put("acks", "all");
		props.put("linger.ms", 1);
		
		//partitioner.class
		return new KafkaProducer<>(props);
	}
	
	static void	display_topic(Producer<String, String> producer, String topic) {
		List<PartitionInfo> list = producer.partitionsFor(topic);
		list.stream().forEach(System.out::print);	
		System.out.println("");
	}
	
	static void	sending_topic(Producer<String, String> producer, String topic) {
		//producer.send(new ProducerRecord<String, String>(topic, "\n<==="));
		for(int i = 0; i < 10; i++) {
			System.out.println("send " + i);
			producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i * 10)));
			//Thread.sleep(1000);
		}
		//producer.send(new ProducerRecord<String, String>(topic, "===>\n"));
	}

	static void	sending_handle(Producer<String, String> producer, String topic) {
		Callback handle = (RecordMetadata metadata, Exception exception) -> {
			System.out.println("call back, " + metadata + ", time " + metadata.timestamp());
		};
		
		for(int i = 0; i < 10; i++) {
			System.out.println("send " + i);
			producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i * 10)), handle);
		}
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String topic = "test";
		
		Producer<String, String> producer = getProducer("192.168.36.10:9092");
		
		/** get inner state */
		display_topic(producer, topic);
		
		sending_topic(producer, topic);
	
		sending_handle(producer, topic);
		
		/** no need, just for test */
		producer.flush();
		
		System.out.println("Message sent successfully");
		producer.close();
	}
}
