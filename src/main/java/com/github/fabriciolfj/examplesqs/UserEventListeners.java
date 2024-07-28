package com.github.fabriciolfj.examplesqs;

import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static io.awspring.cloud.sqs.listener.SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP;

@Component
public class UserEventListeners {

    private static final Logger logger = LoggerFactory.getLogger(UserEventListeners.class);

    public static final String EVENT_TYPE_CUSTOM_HEADER = "eventType";

    private final UserRepository userRepository;

    public UserEventListeners(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @SqsListener("${events.queues.user-created-by-name-queue}")
    public void receiveStringMessage(final String username) {
        logger.info("Received message: {}", username);
        userRepository.save(new User(UUID.randomUUID()
                .toString(), username, null));
    }

    @SqsListener("${events.queues.user-created-record-queue}")
    public void receiveRecordMessage(final UserCreatedEvent event) {
        logger.info("receive message: {}", event);
        userRepository.save(new User(event.id(), event.username(), event.email()));
    }

    @SqsListener("${events.queues.user-created-event-type-queue}")
    public void customHeaderMessage(Message<UserCreatedEvent> message, @Header(EVENT_TYPE_CUSTOM_HEADER) String eventType,
                                    @Header(SQS_APPROXIMATE_FIRST_RECEIVE_TIMESTAMP) Long firstReceive) {
        logger.info("Received message {} with event type {}. First received at approximately {}.", message, eventType, firstReceive);
        UserCreatedEvent payload = message.getPayload();
        userRepository.save(new User(payload.id(), payload.username(), payload.email()));
    }

    @SqsListener("${events.queues.shipping.custom-object-mapper-queue}")
    public void receiveShipmentRequestWithCustomObjectMapper(ShipmentRequestedEvent shipmentRequestedEvent) {
        logger.info("receive shipment request with custom object mapper: {}", shipmentRequestedEvent);
    }


}

