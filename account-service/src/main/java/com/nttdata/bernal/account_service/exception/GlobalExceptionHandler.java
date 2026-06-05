package com.nttdata.bernal.account_service.exception;

import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleResourceNotFound(ResourceNotFoundException exception) {
        return Map.of("message", exception.getMessage());
    }

    @ExceptionHandler(BusinessRuleException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleBusinessRule(BusinessRuleException exception) {
        return Map.of("message", exception.getMessage());
    }
}
