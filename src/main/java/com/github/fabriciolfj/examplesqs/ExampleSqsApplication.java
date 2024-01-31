package com.github.fabriciolfj.examplesqs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(EventQueuesProperties.class)
public class ExampleSqsApplication {

	public static void main(String[] args) {
		SpringApplication.run(ExampleSqsApplication.class, args);
	}

}
