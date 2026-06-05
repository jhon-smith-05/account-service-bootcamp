package com.nttdata.bernal.account_service.service;


import com.nttdata.bernal.account_service.exception.BusinessRuleException;
import com.nttdata.bernal.account_service.exception.ResourceNotFoundException;
import com.nttdata.bernal.account_service.model.*;
import com.nttdata.bernal.account_service.repository.AccountMovementRepository;
import com.nttdata.bernal.account_service.repository.AccountRepository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Service;
import reactor.adapter.rxjava.RxJava3Adapter;
import reactor.core.publisher.Mono;

@Slf4j
@Service
@RefreshScope
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final AccountMovementRepository movementRepository;

    @Value("${bank.account.savings-monthly-movement-limit:20}")
    private int savingsMonthlyMovementLimit;

    public Single<Account> create(Account account) {
        log.debug("Creating {} account for customer {}", account.getType(), account.getCustomerId());
        Mono<Account> result = validateAccountCreation(account)
                .then(accountRepository.save(normalize(account)));
        return RxJava3Adapter.monoToSingle(result);
    }

    public Flowable<Account> findAll() {
        return RxJava3Adapter.fluxToFlowable(accountRepository.findAll());
    }

    public Single<Account> findById(String id) {
        return RxJava3Adapter.monoToSingle(findAccount(id));
    }

    public Single<Account> update(String id, Account account) {
        Mono<Account> result = findAccount(id)
                .flatMap(current -> {
                    current.setMaintenanceFee(account.getMaintenanceFee());
                    current.setMonthlyMovementLimit(account.getMonthlyMovementLimit());
                    current.setAllowedMovementDay(account.getAllowedMovementDay());
                    current.setHolders(account.getHolders());
                    current.setAuthorizedSigners(account.getAuthorizedSigners());
                    return accountRepository.save(current);
                });
        return RxJava3Adapter.monoToSingle(result);
    }

    public Completable delete(String id) {
        return RxJava3Adapter.monoToCompletable(findAccount(id).flatMap(accountRepository::delete));
    }

    public Single<AccountMovement> deposit(String accountId, BigDecimal amount) {
        return registerMovement(accountId, MovementType.DEPOSIT, amount);
    }

    public Single<AccountMovement> withdraw(String accountId, BigDecimal amount) {
        return registerMovement(accountId, MovementType.WITHDRAWAL, amount);
    }

    public Single<BigDecimal> getBalance(String accountId) {
        return RxJava3Adapter.monoToSingle(findAccount(accountId).map(Account::getBalance));
    }

    public Flowable<AccountMovement> findMovements(String accountId) {
        return RxJava3Adapter.fluxToFlowable(movementRepository.findByAccountId(accountId));
    }

    private Single<AccountMovement> registerMovement(String accountId, MovementType type, BigDecimal amount) {
        Mono<AccountMovement> result = findAccount(accountId)
                .flatMap(account -> validateMovement(account, type, amount)
                        .then(saveMovement(account, type, amount)));
        return RxJava3Adapter.monoToSingle(result);
    }

    private Mono<Void> validateAccountCreation(Account account) {
        if (account.getCustomerType() == CustomerType.BUSINESS
                && (account.getType() == AccountType.SAVINGS || account.getType() == AccountType.FIXED_TERM)) {
            return Mono.error(new BusinessRuleException("Business customers can only open checking accounts"));
        }
        if (account.getCustomerType() == CustomerType.PERSONAL
                && (account.getType() == AccountType.SAVINGS || account.getType() == AccountType.CHECKING)) {
            return accountRepository.existsByCustomerIdAndType(account.getCustomerId(), account.getType())
                    .flatMap(exists -> exists
                            ? Mono.error(new BusinessRuleException("Personal customer already has this account type"))
                            : Mono.empty());
        }
        return Mono.empty();
    }

    private Account normalize(Account account) {
        if (account.getBalance() == null) {
            account.setBalance(BigDecimal.ZERO);
        }
        if (account.getType() == AccountType.SAVINGS) {
            account.setMaintenanceFee(BigDecimal.ZERO);
            account.setMonthlyMovementLimit(savingsMonthlyMovementLimit);
        }
        if (account.getType() == AccountType.FIXED_TERM) {
            account.setMaintenanceFee(BigDecimal.ZERO);
        }
        return account;
    }

    private Mono<Void> validateMovement(Account account, MovementType type, BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            return Mono.error(new BusinessRuleException("Movement amount must be greater than zero"));
        }
        if (type == MovementType.WITHDRAWAL && account.getBalance().compareTo(amount) < 0) {
            return Mono.error(new BusinessRuleException("Insufficient account balance"));
        }
        if (account.getType() == AccountType.FIXED_TERM
                && account.getAllowedMovementDay() != null
                && LocalDate.now().getDayOfMonth() != account.getAllowedMovementDay()) {
            return Mono.error(new BusinessRuleException("Fixed term accounts only allow movements on the configured day"));
        }
        if (account.getType() == AccountType.SAVINGS && account.getMonthlyMovementLimit() != null) {
            LocalDate today = LocalDate.now();
            LocalDateTime start = today.withDayOfMonth(1).atStartOfDay();
            LocalDateTime end = today.plusMonths(1).withDayOfMonth(1).atStartOfDay();
            return movementRepository.countByAccountIdAndCreatedAtBetween(account.getId(), start, end)
                    .flatMap(count -> count >= account.getMonthlyMovementLimit()
                            ? Mono.error(new BusinessRuleException("Savings account monthly movement limit reached"))
                            : Mono.empty());
        }
        return Mono.empty();
    }

    private Mono<AccountMovement> saveMovement(Account account, MovementType type, BigDecimal amount) {
        BigDecimal balance = type == MovementType.DEPOSIT
                ? account.getBalance().add(amount)
                : account.getBalance().subtract(amount);
        account.setBalance(balance);
        return accountRepository.save(account)
                .then(movementRepository.save(AccountMovement.builder()
                        .accountId(account.getId())
                        .type(type)
                        .amount(amount)
                        .resultingBalance(balance)
                        .createdAt(LocalDateTime.now())
                        .build()));
    }

    private Mono<Account> findAccount(String id) {
        return accountRepository.findById(id)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("Account not found")));
    }
}
