package com.nttdata.bernal.account_service.repository;


import com.nttdata.bernal.account_service.model.Account;
import com.nttdata.bernal.account_service.model.AccountType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;

public interface AccountRepository extends ReactiveMongoRepository<Account, String> {

    Flux<Account> findByCustomerId(String customerId);

    Mono<Boolean> existsByCustomerIdAndType(String customerId, AccountType type);
}
