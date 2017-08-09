package com.nd.datalake.framework.processor;

import com.nd.datalake.framework.dto.Event;

/**
 * @author krishnaprasad
 *
 */
public interface EventProcessor {

	void destroy();

	Event process(String event);

	void init();

}
