package com.nd.datalake.common.utils;

import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * The JsonExtractor class
 *
 * @author krishnaprasad
 *
 */
public class JsonExtractor {

	public static String extractValuesAsLine(String jsonData, List<String> keys) {
		JsonParser parser = new JsonParser();
		JsonObject jsonObject = (JsonObject) parser.parse(jsonData);
		StringBuilder sb = new StringBuilder();
		for (String key : keys) {
			sb.append(getStringValue(jsonObject, key)).append(",");
		}
		return sb.toString();
	}

	public static String[] extractValues(String jsonData, List<String> keys) {
		String[] values = new String[keys.size()];
		JsonParser parser = new JsonParser();
		JsonObject jsonObject = (JsonObject) parser.parse(jsonData);
		int i = 0;
		for (String key : keys) {
			values[i] = getStringValue(jsonObject, key);
			i++;
		}
		return values;
	}

	private static String getStringValue(JsonObject jsonObject, String key) {
		JsonObject joJsonObj = jsonObject;
		if (key.contains(".")) {
			String[] elements = key.split("\\.");
			int count = 0;
			do {
				joJsonObj = joJsonObj.getAsJsonObject(elements[count]);
				count++;
			} while (count < elements.length - 1);
			key = elements[count];
		}
		JsonElement jsonElement = joJsonObj.get(key);
		return jsonElement.getAsString();
	}

}
