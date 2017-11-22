package com.nd.datalake.stream.sink;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;

import com.nd.datalake.common.parquet.ParquetEventWriter;
import com.nd.datalake.common.parquet.ParquetRecordTranslator;
import com.nd.datalake.common.utils.AvroUtil;
import com.nd.datalake.framework.store.EventStore;
import com.nd.datalake.stream.event.RealtimeEvent;
import com.nd.datalake.stream.event.custom.RealTimeTweet;

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

	@Autowired(required = false)
	private EventStore eventStore;

	@Value("${streamSink.fieldSeparator}")
	private String fieldSeparator;

	@Value("${streamSink.fields}")
	private String filedsToExtract;

	public static void main(String[] args) {
		final SpringApplication springApplication = new SpringApplication(RealtimeStreamSink.class);
		springApplication.setWebEnvironment(false);
		springApplication.setBannerMode(Banner.Mode.OFF);
		springApplication.run();
		String property = System.getProperty("spring.cloud.stream.bindings.input.destination");
		System.out.println(
				"RealtimeStreamSink|Property Value, spring.cloud.stream.bindings.input.destination:" + property);
	}

	@PostConstruct
	public void init() throws InstantiationException, IllegalAccessException {
		logger.info("[RealtimeStreamSink|init|Sink started...]");
	}

	@StreamListener(value = Sink.INPUT)
	public void stream(byte[] feed) {
		try {
			if (feed != null && feed.length > 0) {
				eventStore.store(new String(feed));
				logger.debug("[RealtimeStreamSink|stream|Line processing complete:{}]", feed);
			} else {
				logger.debug("[RealtimeStreamSink|stream|Invalid streamEvent recieved:{}]", feed);
			}

		} catch (Exception e) {
			logger.error("Feed processing failed, Feed:{}", feed, e);
		}
	}

	@PreDestroy
	public void destroy() {
		logger.debug("[RealtimeStreamSink|destroy|Sink destroyed]");
	}

	@Bean(initMethod = "init", destroyMethod = "destroy")
	public com.nd.datalake.stream.event.store.StringToParquetStore eventStore() {
		return new com.nd.datalake.stream.event.store.StringToParquetStore();
	}

	@Autowired
	@Bean(initMethod = "init", destroyMethod = "destroy")
	public com.nd.datalake.common.parquet.ParquetEventWriter<RealtimeEvent> parquetEventWriter(
			@Value("${streamSink.store.paquet.pageSizeBytes:1048576}") int pageSize, ParquetRecordTranslator translator,
			@Value("${streamSink.store.paquet.rollingIntervalMins}") int rollingInterval,
			@Value("${streamSink.store.paquet.outputBaseLocation}") String outputBaseLocation) throws IOException {
		return new ParquetEventWriter<RealtimeEvent>(pageSize, translator, rollingInterval,
				AvroUtil.getSchema(RealTimeTweet.class), outputBaseLocation);
	}

	@Bean
	public com.nd.datalake.stream.event.custom.TweetToParquetEventTranslator translator(
			@Value("${streamSink.fields}") String streamFields) {
		return new com.nd.datalake.stream.event.custom.TweetToParquetEventTranslator(streamFields);
	}
}
