package com.nd.datalake.common.parquet;

/**
 * 
 * @author krishnaprasad
 *
 */
public interface BaseAvroEvent {

	/**
	 *
	 * @return a non null value, empty string if no partition
	 */
	String getPartitionColumnValue();
}
