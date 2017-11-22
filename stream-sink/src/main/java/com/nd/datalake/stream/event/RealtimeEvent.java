package com.nd.datalake.stream.event;

import com.nd.datalake.common.parquet.BaseAvroEvent;
import com.nd.datalake.framework.dto.Event;

/**
 * 
 * @author krishnaprasad
 *
 */
public interface RealtimeEvent extends BaseAvroEvent, Event {

}
