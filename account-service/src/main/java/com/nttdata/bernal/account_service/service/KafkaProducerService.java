package com.nttdata.bernal.account_service.service;

import com.nttdata.bernal.account_service.model.event.AuditEvent;
import com.nttdata.bernal.account_service.model.event.FraudAlertEvent;
import com.nttdata.bernal.account_service.model.event.NotificationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final StreamBridge streamBridge;

    private static final String BINDING_NOTIFICATIONS = "banking-notifications-out-0";
    private static final String BINDING_AUDIT         = "audit-events-out-0";
    private static final String BINDING_FRAUD         = "fraud-alerts-out-0";

    private static final Double FRAUD_THRESHOLD          = 10000.0;
    private static final Double FRAUD_CRITICAL_THRESHOLD = 50000.0;

    public void sendNotification(NotificationEvent event) {
        send(BINDING_NOTIFICATIONS, event);
    }

    public void sendAuditEvent(AuditEvent event) {
        send(BINDING_AUDIT, event);
    }

    public void sendFraudAlert(FraudAlertEvent event) {
        send(BINDING_FRAUD, event);
    }

    public void evaluateAndSendFraudAlert(String customerId, String accountId, BigDecimal amount) {
        if (amount.doubleValue() > FRAUD_THRESHOLD) {
            sendFraudAlert(FraudAlertEvent.builder()
                    .alertId(UUID.randomUUID().toString())
                    .customerId(customerId)
                    .accountId(accountId)
                    .alertType("HIGH_AMOUNT")
                    .amount(amount.doubleValue())
                    .severity(amount.doubleValue() > FRAUD_CRITICAL_THRESHOLD ? "CRITICAL" : "HIGH")
                    .timestamp(LocalDateTime.now())
                    .build());
        }
    }

    private void send(String binding, Object payload) {
        boolean sent = streamBridge.send(binding, payload);
        if (sent) {
            log.info("Event sent to binding {}", binding);
        } else {
            log.error("Failed to send event to binding {}", binding);
        }
    }
}
