/**
 * 
 */
package com.nd.datalake.common.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author krish
 *
 */
public class StringUtil {

	public static List<String> lineToList(String separator, String line) {
		String[] fields = line.split(separator);
		List<String> fieldList = new ArrayList<String>(fields.length);
		for (String field : fields) {
			fieldList.add(field.trim());
		}
		return fieldList;
	}
}
