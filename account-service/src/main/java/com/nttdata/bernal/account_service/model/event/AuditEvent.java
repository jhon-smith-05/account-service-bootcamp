package com.nttdata.bernal.account_service.model.event;

import lombok.*;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditEvent {
    private String eventId;
    private String serviceOrigin;
    private String action;
    private String entityId;
    private String userId;
    private LocalDateTime timestamp;
    private Map<String, Object> metadata;
}
