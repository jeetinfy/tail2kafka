package com.cisco.kafka;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.log4j.Logger;

public class FileChangeListener extends TailerListenerAdapter {

	static final Logger logger = Logger.getLogger(FileChangeListener.class);
	
	private TTKProducer ttkProducer = TTKProducer.getInstance(); 

	@Override
	public void handle(String line) {

		try {
			int lineLength = line.length();
			if (lineLength > Tail2Kafka.BATCH_SIZE) {
				logger.info("BATCH_SIZE" + Tail2Kafka.BATCH_SIZE);
				logger.info("Line length: " + lineLength);
				logger.info("Line length is greater than batch size so breaking line and sending in chunks");
				int startIndex = 1;
				int endIndex = Tail2Kafka.BATCH_SIZE;
				while (startIndex < lineLength) {
					if (endIndex > lineLength) {
						endIndex = lineLength;
					}
					String strTemp = line.substring(startIndex - 1, endIndex);
					logger.info("Sending : " + strTemp.length() + " Byte");
					// logger.info(logBuilder.toString());
					ttkProducer.sendMessage(strTemp);
					startIndex += Tail2Kafka.BATCH_SIZE;
					endIndex += Tail2Kafka.BATCH_SIZE;
				}
			} else {
				logger.info("Sending : " + line.length() + " Byte");
				// logger.info(logBuilder.toString());
				ttkProducer.sendMessage(line);
			}

		} catch (Exception e) {
			logger.error("Exception", e);
		}
	}
}
