package com.nttdata.bernal.account_service.repository;

import java.time.LocalDateTime;
import com.nttdata.bernal.account_service.model.AccountMovement;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface AccountMovementRepository extends ReactiveMongoRepository<AccountMovement, String> {

    Flux<AccountMovement> findByAccountId(String accountId);

    Mono<Long> countByAccountIdAndCreatedAtBetween(String accountId, LocalDateTime start, LocalDateTime end);
}
