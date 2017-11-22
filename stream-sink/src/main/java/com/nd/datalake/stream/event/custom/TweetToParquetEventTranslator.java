/**
 * 
 */
package com.nd.datalake.stream.event.custom;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.nd.datalake.common.parquet.ParquetRecordTranslator;
import com.nd.datalake.common.utils.JsonExtractor;
import com.nd.datalake.common.utils.StringUtil;

/**
 * @author krish
 *
 */
public class TweetToParquetEventTranslator implements ParquetRecordTranslator {
	private Schema avroSchema;
	private List<String> streamMsgFieldNames;

	public TweetToParquetEventTranslator(String fieldNames) {
		super();
		this.streamMsgFieldNames = StringUtil.lineToList(",", fieldNames);
	}

	@Override
	public void setAvroSchema(Schema schema) {
		this.avroSchema = schema;
	}

	@Override
	public GenericRecord translate(String event) {
		String[] extractedValues = JsonExtractor.extractValues(event, streamMsgFieldNames);
		final GenericRecord record = new GenericData.Record(avroSchema);
		record.put("userA", extractedValues[0]);
		record.put("createdDate", extractedValues[1]);
		record.put("favoriteCount", Integer.parseInt(extractedValues[2]));
		record.put("userB", extractedValues[3]);
		return record;
	}

}
