/**
 * 
 */
package com.nd.datalake.stream.spring;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.nd.datalake.common.parquet.ParquetEventWriter;
import com.nd.datalake.common.parquet.ParquetRecordTranslator;
import com.nd.datalake.common.utils.AvroUtil;
import com.nd.datalake.stream.event.RealtimeEvent;
import com.nd.datalake.stream.event.custom.RealTimeTweet;

/**
 * @author krish
 *
 */
@Configuration
public class StreamSinkContext {

	@Bean(initMethod = "init", destroyMethod = "destroy")
	public com.nd.datalake.stream.event.store.StringToParquetStore eventStore() {
		return new com.nd.datalake.stream.event.store.StringToParquetStore();
	}

	@Autowired
	@Bean(initMethod = "init", destroyMethod = "destroy")
	public com.nd.datalake.common.parquet.ParquetEventWriter<RealtimeEvent> parquetEventWriter(
			@Value("${streamSink.store.paquet.pageSizeBytes}") int pageSize, ParquetRecordTranslator translator,
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
