package com.example.springbatchpartitioner.config;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;


//custom skip policy
public class ExceptionSkipPolicy implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable throwable, int i) throws SkipLimitExceededException {

      //if it is numberformat should skip (same as .skip(NumberformatException.class)
        return throwable instanceof NumberFormatException;
    }
}
