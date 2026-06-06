package com.nttdata.bernal.account_service.model.event;

import lombok.*;

import java.time.LocalDateTime;

@Data
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEvent {
    private String customerId;
    private String accountId;
    private String type;
    private String channel;
    private String message;
    private LocalDateTime timestamp;
}
