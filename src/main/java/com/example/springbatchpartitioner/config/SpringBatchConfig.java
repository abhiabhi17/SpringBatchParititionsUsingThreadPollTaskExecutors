package com.example.springbatchpartitioner.config;

import com.example.springbatchpartitioner.entity.Customer;
import com.example.springbatchpartitioner.partition.ColumnRangePartitioner;
import com.example.springbatchpartitioner.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import net.bytebuddy.utility.nullability.MaybeNull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {


    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
//    private CustomerRepository customerRepository;
    private  CustomerWriter customerWriter;


    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomers")
                .flow(masterStep()).end().build();
    }
    @Bean
    public Step masterStep()
    {
        return stepBuilderFactory.get("masterStep")
                .partitioner(slaveStep().getName(),partitioner())
                .partitionHandler(partitonHandlder())
                .build();
    }
    @Bean
    public Step slaveStep()
    {
        // step name is csv-step
        return stepBuilderFactory.get("csv-step").<Customer,Customer>chunk(500)
                .reader(reader())
                .processor(processor())
                .writer(customerWriter)
                .build();
    }


    @Bean // Manullay user has to create a bean object when the class is @confirguration
    public FlatFileItemReader<Customer>  reader()
    {
        FlatFileItemReader<Customer> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));
        itemReader.setName("csvReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }
    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");


        BeanWrapperFieldSetMapper<Customer> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return  lineMapper;
    }

    @Bean
    public  CustomerProcessor processor()
    {
        return  new CustomerProcessor();
    }

//    @Bean
//    public RepositoryItemWriter<Customer> writer()
//    {
//        RepositoryItemWriter<Customer> writer=new RepositoryItemWriter<>();
//        writer.setRepository(customerRepository);
//        writer.setMethodName("save");
//        return writer;
//    }

    @Bean
   public ColumnRangePartitioner partitioner()
   {
       return new ColumnRangePartitioner();
   }

   @Bean
   public PartitionHandler partitonHandlder()
   {
       TaskExecutorPartitionHandler taskExecutorPartitionHandler=new TaskExecutorPartitionHandler();
       taskExecutorPartitionHandler.setGridSize(4);
       taskExecutorPartitionHandler.setTaskExecutor(taskExecutor() );
       taskExecutorPartitionHandler.setStep(slaveStep());
       return taskExecutorPartitionHandler;
   }

    @Bean
    public TaskExecutor taskExecutor()
    {
//        SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor();
//        asyncTaskExecutor.setConcurrencyLimit(10);
//        return  asyncTaskExecutor;

        ThreadPoolTaskExecutor threadPoolTaskExecutor=new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setMaxPoolSize(4);
        threadPoolTaskExecutor.setCorePoolSize(4);
        threadPoolTaskExecutor.setQueueCapacity(4);
        return  threadPoolTaskExecutor;

    }

}


