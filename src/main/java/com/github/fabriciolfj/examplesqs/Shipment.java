package com.github.fabriciolfj.examplesqs;

import lombok.*;

import java.time.LocalDate;
import java.util.UUID;

@Getter
@Setter
public class Shipment {

    private UUID orderId;
    private String customerAddress;
    private LocalDate shipBy;
    private ShipmentStatus status;

    public Shipment(){}

    public Shipment(UUID orderId, String customerAddress, LocalDate shipBy, ShipmentStatus status) {
        this.orderId = orderId;
        this.customerAddress = customerAddress;
        this.shipBy = shipBy;
        this.status = status;
    }
}
