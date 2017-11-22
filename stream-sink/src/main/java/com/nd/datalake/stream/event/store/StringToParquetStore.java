package com.nd.datalake.stream.event.store;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;

import com.nd.datalake.common.parquet.ParquetEventWriter;
import com.nd.datalake.framework.store.EventStore;
import com.nd.datalake.stream.event.RealtimeEvent;

public class StringToParquetStore implements EventStore {
	@Autowired
	private ParquetEventWriter<RealtimeEvent> parquetWriter;

	public void init() {
		// TODO Auto-generated method stub

	}

	@Override
	public void store(String feed) {
		try {
			parquetWriter.write(feed);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void destroy() {
		// TODO Auto-generated method stub

	}

}
