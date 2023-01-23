package com.spark.convetor.messaging;

import com.spark.convetor.service.ConverterService;
import com.spark.convetor.service.SyncTemperatureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncEventListener {
    private final RabbitTemplate rabbitTemplate;
    private final SyncTemperatureService syncTemperatureService;


    @RabbitListener(queues = "q.sync-weather-queue")
    public void onSyncEvent(SyncEvent event) {
        log.info("Sync temperature since {}", event.getStartDate());
        syncTemperatureService.syncData(event);
    }
}
