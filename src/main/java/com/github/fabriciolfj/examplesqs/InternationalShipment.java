package com.github.fabriciolfj.examplesqs;

import lombok.*;

import java.time.LocalDate;
import java.util.UUID;

@Setter
@Getter
public class InternationalShipment extends Shipment {

    private String destinationCountry;
    private String customsInfo;

    public InternationalShipment(UUID orderId, String customerAddress, LocalDate shipBy, ShipmentStatus status,
                                 String destinationCountry, String customsInfo) {
        super(orderId, customerAddress, shipBy, status);
        this.destinationCountry = destinationCountry;
        this.customsInfo = customsInfo;
    }
}
