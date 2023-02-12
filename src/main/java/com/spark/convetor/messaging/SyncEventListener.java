package com.spark.convetor.messaging;

import com.spark.convetor.service.SyncTemperatureService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncEventListener {
    private final SyncTemperatureService syncTemperatureService;


    @KafkaListener(
            topics = "${spring.kafka.template.default-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onSyncEvent(SyncEvent event) {
        log.info("Sync temperature since {}", event.getStartDate());
        syncTemperatureService.syncData(event);
    }
}
