package com.nttdata.bernal.account_service.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "account_movements")
public class AccountMovement {

    @Id
    private String id;

    private String accountId;
    private MovementType type;
    private BigDecimal amount;
    private BigDecimal resultingBalance;
    private LocalDateTime createdAt;
}
