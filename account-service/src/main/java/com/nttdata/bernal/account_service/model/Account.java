package com.nttdata.bernal.account_service.model;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
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
@Document(collection = "accounts")
public class Account {

    @Id
    private String id;

    private String customerId;
    private CustomerType customerType;
    private AccountType type;
    private BigDecimal balance;
    private BigDecimal maintenanceFee;
    private Integer monthlyMovementLimit;
    private Integer allowedMovementDay;

    @Builder.Default
    private List<AccountHolder> holders = new ArrayList<>();

    @Builder.Default
    private List<AccountHolder> authorizedSigners = new ArrayList<>();
}
