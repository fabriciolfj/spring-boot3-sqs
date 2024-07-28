package com.github.fabriciolfj.examplesqs;

import lombok.*;

import java.time.LocalDate;
import java.util.UUID;

@Setter
@Getter
public class DomesticShipment extends Shipment {

    private String deliveryRouteCode;

    public DomesticShipment(UUID orderId, String customerAddress, LocalDate shipBy, ShipmentStatus status,
                            String deliveryRouteCode) {
        super(orderId, customerAddress, shipBy, status);
        this.deliveryRouteCode = deliveryRouteCode;
    }

    public String getDeliveryRouteCode() {
        return deliveryRouteCode;
    }

    public void setDeliveryRouteCode(String deliveryRouteCode) {
        this.deliveryRouteCode = deliveryRouteCode;
    }
}
