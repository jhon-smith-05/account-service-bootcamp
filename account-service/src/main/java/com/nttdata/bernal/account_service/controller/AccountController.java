package com.nttdata.bernal.account_service.controller;

import com.nttdata.bernal.account_service.model.Account;
import com.nttdata.bernal.account_service.model.AccountMovement;
import com.nttdata.bernal.account_service.service.AccountService;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import java.math.BigDecimal;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Single<Account> create(@RequestBody Account account) {
        return accountService.create(account);
    }

    @GetMapping
    public Flowable<Account> findAll() {
        return accountService.findAll();
    }

    @GetMapping("/{id}")
    public Single<Account> findById(@PathVariable("id") String id) {
        return accountService.findById(id);
    }

    @PutMapping("/{id}")
    public Single<Account> update(@PathVariable("id") String id, @RequestBody Account account) {
        return accountService.update(id, account);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Completable delete(@PathVariable String id) {
        return accountService.delete(id);
    }

    @PostMapping("/{id}/deposits")
    public Single<AccountMovement> deposit(@PathVariable("id") String id, @RequestBody Map<String, BigDecimal> request) {
        return accountService.deposit(id, request.get("amount"));
    }

    @PostMapping("/{id}/withdrawals")
    public Single<AccountMovement> withdraw(@PathVariable("id") String id, @RequestBody Map<String, BigDecimal> request) {
        return accountService.withdraw(id, request.get("amount"));
    }

    @GetMapping("/{id}/balance")
    public Single<Map<String, BigDecimal>> getBalance(@PathVariable("id") String id) {
        return accountService.getBalance(id).map(balance -> Map.of("availableBalance", balance));
    }

    @GetMapping("/{id}/movements")
    public Flowable<AccountMovement> findMovements(@PathVariable("id") String id) {
        return accountService.findMovements(id);
    }
}
