package com.example.springbatchpartitioner.config;

import com.example.springbatchpartitioner.entity.Customer;
import com.example.springbatchpartitioner.listener.StepSkipListener;
import com.example.springbatchpartitioner.partition.ColumnRangePartitioner;
import com.example.springbatchpartitioner.repository.CustomerRepository;
import lombok.AllArgsConstructor;
import net.bytebuddy.utility.nullability.MaybeNull;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.step.skip.SkipPolicy;
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

    /*  Job have Steps
            Steps have Iteam Reader- Item Processor-Item Writer
        */


    private JobBuilderFactory jobBuilderFactory;
    private StepBuilderFactory stepBuilderFactory;
//    private CustomerRepository customerRepository;
    private  CustomerWriter customerWriter;


    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomers")
                .flow(masterStep()).end().build();
    }

    /* In Step We have to give reader () bean, writer bean which is of customerItemWriter Which implements ItemWriter
     *  Fault Tolerance is like exception handling , if any exceptions occured during reading or wrting csv file ,process of writing to database should not stop
     * skip method is for to skip  that special kind of exception  like number format exception
     * Listner class is providing bean of skiplisten which implements skip listeenr
     * which listens where the excpetion occurd whter it is in itemreder or item writer or item processor */
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
                .faultTolerant()//If any exception in ilegal value in any field in csv file it skips that row and inserts others
//                .skipLimit(100)
//                .skip(Exception.class)//Skip any exceptions
//                .noSkip(IllegalArgumentException.class)//dont skip (records should not insert)
                .listener(skipListener())
                .skipPolicy(skipPolicy())
                .build();
    }

    /* Skip Listener is an interface which listens all the exceptions where occured in item reader or item writer or processor  */
    @Bean
    public SkipListener skipListener(){
        return new StepSkipListener();
    }

    /* Skip Policy  is an interface is used to write our custom skip exceptions just like .skip(NumberformatException.class) */
    @Bean
    public SkipPolicy skipPolicy() {
        return  new ExceptionSkipPolicy();
    }


    /* Item Reader is used to read the  the data from csv file
     * skip lines is used to skip the header lien of csv file
     * line mapper bean is used to read the data line by line  */
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

    /* Item Reader is used to read the  the data from csv file
     * skip lines is used to skip the header lien of csv file
     * line mapper bean is used to read the data line by line
     * LineTokenizer sperates with comma value */
    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> lineMapper=new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id","firstName","lastName","email","gender","contactNo","country","dob");

        /* BeanWrapperField is used to map the csv file oject to customer object  */
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return  lineMapper;
    }
    /* This process bean is used to process  */
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


    /* this bean implements partitioner  based on grid size it divides  */
    @Bean
   public ColumnRangePartitioner partitioner()
   {
       return new ColumnRangePartitioner();
   }


    /* TaskExecutorPartitionHandler is used to divied based on grid size   */
   @Bean
   public PartitionHandler partitonHandlder()
   {
       TaskExecutorPartitionHandler taskExecutorPartitionHandler=new TaskExecutorPartitionHandler();
       taskExecutorPartitionHandler.setGridSize(4);
       taskExecutorPartitionHandler.setTaskExecutor(taskExecutor() );
       taskExecutorPartitionHandler.setStep(slaveStep());
       return taskExecutorPartitionHandler;
   }

    /* TaskExecutor is used to provide pool size    */
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


