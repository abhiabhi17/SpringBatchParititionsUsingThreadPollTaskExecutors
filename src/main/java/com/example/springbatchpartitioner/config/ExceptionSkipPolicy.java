package com.example.springbatchpartitioner.config;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;


/*
 * Skip Policy is used for custom implemenation of skip the exceptions like number formati exception
 *  i.e if any eroor or exceptions like number format or illegal argument exception this class will handle
 *   usually we declare as .skip(NumberFormatiException.class)
 *
 *  */
public class ExceptionSkipPolicy implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable throwable, int i) throws SkipLimitExceededException {

      //if it is numberformat should skip (same as .skip(NumberformatException.class)
        return throwable instanceof NumberFormatException;
    }
}
