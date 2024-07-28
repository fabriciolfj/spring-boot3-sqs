package com.github.fabriciolfj.examplesqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

import static com.github.fabriciolfj.examplesqs.UserEventListeners.EVENT_TYPE_CUSTOM_HEADER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

public class SpringCloudAwsSQSLiveTest extends BaseSqsIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(SpringCloudAwsSQSLiveTest.class);

    @Autowired
    private SqsTemplate sqsTemplate;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private EventQueuesProperties eventQueuesProperties;

    @Autowired
    private SqsAsyncClient sqsAsyncClient;

    @Autowired
    private ShipmentEventsQueuesProperties shipmentEventsQueuesProperties;

    @Autowired
    private ShippingHeaderTypesProperties headerTypesProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void givenShipmentRequestWithCustomDateFormat_whenMessageReceived_thenDeserializesDateCorrectly() {
        UUID orderId = UUID.randomUUID();
        String shipBy = LocalDate.parse("2024-05-12")
                .format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));

        var jsonMessage = """
            {
                "orderId": "%s",
                "customerAddress": "123 Main St",
                "shipBy": "%s"
            }
            """.formatted(orderId, shipBy);

        sendRawMessage(shipmentEventsQueuesProperties.getCustomObjectMapperQueue(), jsonMessage);
    }

    private void sendRawMessage(String queueName, String jsonMessage) {
        sqsAsyncClient.getQueueUrl(req -> req.queueName(queueName))
                .thenCompose(resp -> sqsAsyncClient.sendMessage(req -> req.messageBody(jsonMessage)
                        .queueUrl(resp.queueUrl())))
                .join();
    }



    @Test
    void givenAStringPayload_whenSend_shouldReceive() {
        var userName = "Fabricio";

        sqsTemplate.send(t -> t.queue(eventQueuesProperties.getUserCreatedByNameQueue()).payload(userName));

        logger.info("message sent with payload {}", userName);

        await().atMost(Duration.ofSeconds(3))
                .until(() -> userRepository.findByName(userName).isPresent());
    }

    @Test
    void givenARecordPayload_whenSend_shouldReceive() {
        // given
        String userId = UUID.randomUUID()
                .toString();
        var payload = new UserCreatedEvent(userId, "Fabricio", "fabricio.jacob@outlook.com");

        // when
        sqsTemplate.send(to -> to.queue(eventQueuesProperties.getUserCreatedRecordQueue())
                .payload(payload));

        // then
        logger.info("Message sent with payload: {}", payload);
        await().atMost(Duration.ofSeconds(3))
                .until(() -> userRepository.findById(userId)
                        .isPresent());
    }

    @Test
    void givenCustomHeaders_whenSend_shouldReceive() {
        // given
        String userId = UUID.randomUUID()
                .toString();
        var payload = new UserCreatedEvent(userId, "fabricio", "fabricio@outlook.com");
        var headers = Map.<String, Object> of(EVENT_TYPE_CUSTOM_HEADER, "UserCreatedEvent");

        // when
        sqsTemplate.send(to -> to.queue(eventQueuesProperties.getUserCreatedEventTypeQueue())
                .payload(payload)
                .headers(headers));

        // then
        logger.info("Sent message with payload {} and custom headers: {}", payload, headers);
        await().atMost(Duration.ofSeconds(3))
                .until(() -> userRepository.findById(userId)
                        .isPresent());
    }


    @Test
    void givenPayloadWithSubclasses_whenMessageReceived_thenDeserializesCorrectType() {
        var domesticOrderId = UUID.randomUUID();
        String deliveryRouteCode = "XPTO1234";
        var domesticEvent = new DomesticShipmentRequestedEvent(domesticOrderId, "123 Main St", LocalDate.parse("2024-05-12"), deliveryRouteCode);

        var internationalOrderId = UUID.randomUUID();
        String destinationCountry = "Canada";
        String customsInfo = "HS Code: 8471.30, Origin: China, Value: $500";
        InternationalShipmentRequestedEvent internationalEvent = new InternationalShipmentRequestedEvent(internationalOrderId, "123 Main St", LocalDate.parse("2024-05-24"),
                destinationCountry, customsInfo);

        var customTemplate = SqsTemplate.builder()
                .sqsAsyncClient(sqsAsyncClient)
                .configureDefaultConverter(converter -> {
                    converter.doNotSendPayloadTypeHeader();
                    converter.setObjectMapper(objectMapper);
                })
                .build();

        customTemplate.send(to -> to.queue(shipmentEventsQueuesProperties.getSubclassDeserializationQueue())
                .payload(internationalEvent)
                .header(headerTypesProperties.getHeaderName(), headerTypesProperties.getInternational()));

        customTemplate.send(to -> to.queue(shipmentEventsQueuesProperties.getSubclassDeserializationQueue())
                .payload(domesticEvent)
                .header(headerTypesProperties.getHeaderName(), headerTypesProperties.getDomestic()));
    }

}
