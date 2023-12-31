package com.example.springbatchpartitioner.config;

import com.example.springbatchpartitioner.entity.Customer;
import com.example.springbatchpartitioner.repository.CustomerRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/*
 This Class is used to write to the repository database from csv file

 */
@Component
public class CustomerWriter implements ItemWriter<Customer> {

    @Autowired
    private CustomerRepository customerRepository;

    @Override
    public void write(List<? extends Customer> list) throws Exception {
        System.out.println("Thread Name: " +Thread.currentThread().getName());
        customerRepository.saveAll(list);
    }
}
