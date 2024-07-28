package com.github.fabriciolfj.examplesqs;

import lombok.Getter;
import lombok.Setter;
import java.time.LocalDate;
import java.util.UUID;

@Getter
@Setter
public class ShipmentRequestedEvent {

    private UUID orderId;
    private String customerAddress;
    private LocalDate shipBy;

    public ShipmentRequestedEvent() {
    }

    public ShipmentRequestedEvent(UUID orderId, String customerAddress, LocalDate shipBy) {
        this.orderId = orderId;
        this.customerAddress = customerAddress;
        this.shipBy = shipBy;
    }

    public Shipment toDomain() {
        return new Shipment(orderId, customerAddress, shipBy, ShipmentStatus.REQUESTED);
    }

}
