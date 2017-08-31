package com.nd.datalake.stream.sink;

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

import com.nd.datalake.framework.store.EventStore;
import com.nd.datalake.stream.spring.StreamSinkContext;

/**
 * The RealtimeStreamSink class
 *
 * @author krishnaprasad
 *
 */
@SpringBootApplication
@EnableBinding(Sink.class)
public class RealtimeStreamSink {
	private Logger logger = LoggerFactory.getLogger(RealtimeStreamSink.class);
	@Autowired
	private EventStore eventStore;

	@Value("${streamSink.fieldSeparator}")
	private String fieldSeparator;

	@Value("${streamSink.fields}")
	private String filedsToExtract;

	public static void main(String[] args) {
		final SpringApplication springApplication = new SpringApplication(StreamSinkContext.class);
		springApplication.setWebEnvironment(false);
		springApplication.run(args);
		String property = System.getProperty("spring.cloud.stream.bindings.input.destination");
		System.out.println(
				"RealtimeStreamSink|Property Value, spring.cloud.stream.bindings.input.destination:" + property);
	}

	@PostConstruct
	public void init() throws InstantiationException, IllegalAccessException {
		logger.info("[RealtimeStreamSink|init|Sink started...]");
	}

	@StreamListener(value = Sink.INPUT)
	public void stream(String feed) {
		try {
			eventStore.store(feed);
			logger.debug("Line processing complete:{}", feed);
		} catch (Exception e) {
			logger.error("Feed processing failed, Feed:{}", feed, e);
		}
	}

	@PreDestroy
	public void destroy() {
		logger.debug("[RealtimeStreamSink|destroy|Sink destroyed]");
	}

}
