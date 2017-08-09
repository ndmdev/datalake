package com.nd.datalake.common.parquet;

/**
 * 
 * @author krishnaprasad
 *
 */
public interface BaseAvroRecord {

	/**
	 *
	 * @return a non null value, empty string if no partition
	 */
	String getPartitionColumnValue();
}
