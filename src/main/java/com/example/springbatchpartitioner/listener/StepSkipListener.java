package com.example.springbatchpartitioner.listener;

import com.example.springbatchpartitioner.entity.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

import java.util.logging.Logger;

/* Skip listener is used to listen all the exception where it is occured whter it is in itemreader,itemwriter or processor   */
public class StepSkipListener implements SkipListener<Customer,Number> {

    //Logger logger = (Logger) (Logger) LoggerFactory.getLogger(StepSkipListener.class);

    @Override // item reader
    public void onSkipInRead(Throwable throwable) {
        System.out.println("In ITEM Reader " +throwable.getMessage());
    }

    @Override // item writter
    public void onSkipInWrite(Number item, Throwable throwable) {
        System.out.println("IN Item WRITER " +throwable.getMessage());
    }

    @SneakyThrows
    @Override // item processor
    public void onSkipInProcess(Customer customer, Throwable throwable) {
        System.out.println("IN Process " +throwable.getMessage());

    }
}
