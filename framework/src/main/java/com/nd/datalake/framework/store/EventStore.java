package com.nd.datalake.framework.store;

public interface EventStore {
	void init();

	void store(String event);

	void destroy();
}
