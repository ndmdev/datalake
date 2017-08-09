package com.nd.datalake.framework.store;

import com.nd.datalake.framework.dto.Event;

public interface EventStore {
	void init();

	void store(Event event);

	void destroy();
}
