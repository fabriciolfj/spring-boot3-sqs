package com.github.fabriciolfj.examplesqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import io.awspring.cloud.sqs.MessageHeaderUtils;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.support.converter.SqsMessagingMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

@SpringBootApplication
@EnableConfigurationProperties({EventQueuesProperties.class, ShipmentEventsQueuesProperties.class, ShippingHeaderTypesProperties.class})
public class ExampleSqsApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExampleSqsApplication.class, args);
	}

	@Autowired
	private ShippingHeaderTypesProperties typesProperties;

	@Autowired
	private SqsAsyncClient sqsAsyncClient;


	@Bean
	public ObjectMapper objectMapper() {
		ObjectMapper mapper = new ObjectMapper();
		JavaTimeModule module = new JavaTimeModule();
		LocalDateDeserializer customDeserializer = new LocalDateDeserializer(DateTimeFormatter.ofPattern("dd-MM-yyyy", Locale.getDefault()));
		module.addDeserializer(LocalDate.class, customDeserializer);
		mapper.registerModule(module);
		return mapper;
	}

	@Bean
	public SqsMessageListenerContainerFactory defaultSqsListenerContainerFactory(ObjectMapper objectMapper) {
		SqsMessagingMessageConverter converter = new SqsMessagingMessageConverter();
		converter.setPayloadTypeMapper(message -> {
			if (!message.getHeaders()
					.containsKey(typesProperties.getHeaderName())) {
				return Object.class;
			}
			String eventTypeHeader = MessageHeaderUtils.getHeaderAsString(message, typesProperties.getHeaderName());
			if (eventTypeHeader.equals(typesProperties.getDomestic())) {
				return DomesticShipmentRequestedEvent.class;
			} else if (eventTypeHeader.equals(typesProperties.getInternational())) {
				return InternationalShipmentRequestedEvent.class;
			}
			throw new RuntimeException("Invalid shipping type");
		});
		converter.setObjectMapper(objectMapper);

		return SqsMessageListenerContainerFactory.builder()
				.sqsAsyncClient(sqsAsyncClient)
				.configure(configure -> configure.messageConverter(converter))
				.build();
	}



}
