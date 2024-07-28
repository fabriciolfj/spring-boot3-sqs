package com.github.fabriciolfj.examplesqs;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "events.queues.shipping")
public class ShipmentEventsQueuesProperties {

    private String simplePojoConversionQueue;

    private String customObjectMapperQueue;

    private String subclassDeserializationQueue;

}
