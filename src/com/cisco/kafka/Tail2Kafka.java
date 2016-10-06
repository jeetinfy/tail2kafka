package com.cisco.kafka;
import java.io.File;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.log4j.Logger;

public class Tail2Kafka {

	static final Logger logger = Logger.getLogger(Tail2Kafka.class);
	public static String TOPIC;
	public static String BOOTSTRAP_SERVER;
	public static final int BATCH_SIZE = 16 * 1024;
	
	public static void main(String[] args) throws Exception {
		
		// Check arguments length value
		if (args.length < 2) {
			logger.info("Syntax");
			logger.info("tail2kafka.jar <Topic> <Kafka Bootstrap Server>");
			return;
		}
		
		TOPIC = args[0];
		BOOTSTRAP_SERVER = args[1];
		
		logger.info("BOOTSTRAP_SERVER: " + BOOTSTRAP_SERVER);
		logger.info("TOPIC: " + TOPIC);
		
		TailerListener listener = new FileChangeListener();
		
		String openShiftLogDir = System.getenv("OPENSHIFT_LOG_DIR");
		logger.info("OPENSHIFT_LOG_DIR: " + openShiftLogDir);
		String logFile = openShiftLogDir + "scminfo.log";
		logger.info("LOG FILE LOCATION: " + logFile);
		Tailer.create(new File(logFile), listener, 3000, true);
		while (true) {
			Thread.sleep(500);
		}
	}

}
