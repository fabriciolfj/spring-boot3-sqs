package com.github.fabriciolfj.examplesqs;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "headers.types.shipping")
public class ShippingHeaderTypesProperties {

    private String headerName;
    private String international;
    private String domestic;

}
