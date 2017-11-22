package com.nd.datalake.common.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A Parquet Avro record translator,
 *
 * @author krishnaprasad
 *
 */
public interface ParquetRecordTranslator {

	GenericRecord translate(String event);

	void setAvroSchema(final Schema schema);

}
