package com.github.fabriciolfj.examplesqs;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@Setter
public class DomesticShipmentRequestedEvent extends ShipmentRequestedEvent {

    private String deliveryRouteCode;

    public DomesticShipmentRequestedEvent(){}

    public DomesticShipmentRequestedEvent(UUID orderId, String customerAddress, LocalDate shipBy, String deliveryRouteCode) {
        super(orderId, customerAddress, shipBy);
        this.deliveryRouteCode = deliveryRouteCode;
    }

    public DomesticShipment toDomain() {
        return new DomesticShipment(getOrderId(), getCustomerAddress(), getShipBy(), ShipmentStatus.REQUESTED, deliveryRouteCode);
    }

}
