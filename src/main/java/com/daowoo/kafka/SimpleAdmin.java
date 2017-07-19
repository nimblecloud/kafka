
package com.daowoo.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;

public class SimpleAdmin {
	
	static Producer<String, String> getAdmin(String server) {
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
		return null;
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
	}
}
