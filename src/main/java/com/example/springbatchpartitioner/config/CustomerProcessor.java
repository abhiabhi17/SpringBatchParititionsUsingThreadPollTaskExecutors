package com.example.springbatchpartitioner.config;

import com.example.springbatchpartitioner.entity.Customer;
import org.springframework.batch.item.ItemProcessor;

/*
 This Class is used to process the data after reading from ITEM-READER

 */
public class CustomerProcessor implements ItemProcessor<Customer,Customer> {

    @Override
    public Customer process(Customer customer) throws Exception {

        /* Process method is custom implemenation according to business logic */
//        if(customer.getCountry().equals("United States")) {
//            return customer;
//        }else{
//            return null;
//        }
        return customer;
    }
}
