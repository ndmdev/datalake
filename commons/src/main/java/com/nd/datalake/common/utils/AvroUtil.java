/**
 * 
 */
package com.nd.datalake.common.utils;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

/**
 * @author krish
 *
 */
public class AvroUtil {
	public static Schema getSchema(Class<?> clazz) {
		return ReflectData.get().getSchema(clazz);
	}
}
