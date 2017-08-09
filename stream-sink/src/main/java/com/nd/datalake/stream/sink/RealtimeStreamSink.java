package com.nd.datalake.stream.sink;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;

import com.nd.datalake.common.utils.JsonExtractor;
import com.nd.datalake.framework.dto.Event;
import com.nd.datalake.framework.processor.EventProcessor;
import com.nd.datalake.framework.store.EventStore;

/**
 * The StreamApplication class
 *
 * @author krishnaprasad
 *
 */
@SpringBootApplication
@EnableBinding(Sink.class)
public class RealtimeStreamSink {
	private Logger logger = LoggerFactory.getLogger(RealtimeStreamSink.class);
	@Autowired
	private EventProcessor eventProcessor;
	@Autowired
	private EventStore eventStore;

	@Value("${streamSink.fieldSeparator}")
	private String fieldSeparator;
	@Value("${streamSink.fields}")
	private String filedsToExtract;
	private List<String> streamMsgFieldNames;

	public static void main(String[] args) {
		final SpringApplication springApplication = new SpringApplication(RealtimeStreamSink.class);
		springApplication.setWebEnvironment(false);
		springApplication.run();
		String property = System.getProperty("spring.cloud.stream.bindings.input.destination");
		System.out
				.println("FlytxtStreamSink|Property Value, spring.cloud.stream.bindings.input.destination:" + property);
	}

	@PostConstruct
	public void init() throws InstantiationException, IllegalAccessException {
		this.streamMsgFieldNames = fieldsAsList(filedsToExtract);
		eventProcessor.init();
		logger.info("[RealtimeStreamSink|init|Sink started...]");
	}

	@StreamListener(value = Sink.INPUT)
	public void loggerSink(String feed) {
		String line = JsonExtractor.extractValues(feed, streamMsgFieldNames);
		try {
			Event event = eventProcessor.process(feed);
			eventStore.store(event);
			logger.debug("Line processing complete:{}", line);
		} catch (Exception e) {
			logger.error("Feed processing failed, Feed:{}", line, e);
		}
	}

	private List<String> fieldsAsList(String streamMsgFieldNames) {
		List<String> fieldNameList = new ArrayList<String>();
		String[] fields = streamMsgFieldNames.split(fieldSeparator);
		for (String field : fields) {
			fieldNameList.add(field.trim());
		}
		return fieldNameList;
	}

	@PreDestroy
	public void destroy() {
		try {
			eventProcessor.destroy();
		} catch (Exception e) {
			throw new RuntimeException("FlytxtStreamSink|destroy|Failed]", e);
		}
	}

}
