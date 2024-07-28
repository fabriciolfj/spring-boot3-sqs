package com.github.fabriciolfj.examplesqs;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@Setter
public class InternationalShipmentRequestedEvent extends ShipmentRequestedEvent {

    private String destinationCountry;
    private String customsInfo;

    public InternationalShipmentRequestedEvent(){}

    public InternationalShipmentRequestedEvent(UUID orderId, String customerAddress, LocalDate shipBy, String destinationCountry,
                                               String customsInfo) {
        super(orderId, customerAddress, shipBy);
        this.destinationCountry = destinationCountry;
        this.customsInfo = customsInfo;
    }

    public InternationalShipment toDomain() {
        return new InternationalShipment(getOrderId(), getCustomerAddress(), getShipBy(), ShipmentStatus.REQUESTED, destinationCountry,
                customsInfo);
    }
}
