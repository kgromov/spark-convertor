package com.spark.convetor.messaging;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
//@Configuration
@ConfigurationProperties(prefix = "rabbitmq")
public class BrokerSettings {
    private String queueName;
    private String exchangeName;
    private String routingKey;
}
