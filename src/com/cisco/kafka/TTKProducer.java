package com.cisco.kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

public class TTKProducer {

	static final Logger logger = Logger.getLogger(TTKProducer.class);

	private static TTKProducer ttkProducer = null;
	
	private Producer<String, String> producer = null;
	
	private TTKProducer() {

	}

	public static TTKProducer getInstance() {

		if (ttkProducer == null) {
			synchronized (TTKProducer.class) {
				if (ttkProducer == null) {
					ttkProducer = new TTKProducer();
					ttkProducer.initProducer();
				}
			}
		}
		return ttkProducer;

	}

	private void initProducer() {
		// create instance for properties to access producer configs
		Properties props = new Properties();

		// Assign localhost id
		props.put("bootstrap.servers", Tail2Kafka.BOOTSTRAP_SERVER + ":9092");

		// Set acknowledgements for producer requests.
		props.put("acks", "all");

		// If the request fails, the producer can automatically retry,
		props.put("retries", 0);

		// Specify buffer size in config
		props.put("batch.size", Tail2Kafka.BATCH_SIZE);
		props.put("compression.type", "snappy");

		// Reduce the no of requests less than 0
		props.put("linger.ms", 2000);

		// The buffer.memory controls the total amount of memory available to the producer for buffering.
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
	}
	
	public void sendMessage(String line) {

		producer.send(new ProducerRecord<String, String>(Tail2Kafka.TOPIC, (new Date()).toString(), line));
		
	}
}

class AckCallBack implements Callback {
	static final Logger logger = Logger.getLogger(AckCallBack.class);
	
	@Override
	public void onCompletion(RecordMetadata arg0, Exception arg1) {
		if(arg0 != null){
			logger.info("Offset: " + arg0.offset());
		}
		if (arg1 != null){
			logger.error("Exception: ", arg1);
		}else{
			logger.info("Message sent successfully");
		}
	}

}