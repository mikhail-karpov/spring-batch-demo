package com.mikhailkarpov.springbatchdemo.billing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.support.JdbcTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BillingJobConfig {

    @Bean
    Step step1(JobRepository jobRepository, JdbcTransactionManager jdbcTransactionManager) {
        return new StepBuilder("filePreparation", jobRepository)
                .tasklet(new FilePreparationTasklet(), jdbcTransactionManager)
                .build();
    }

    @Bean
    FlatFileItemReader<BillingData> billingDataItemReader() {
        return new FlatFileItemReaderBuilder<BillingData>()
                .name("billingDataFileReader")
                .resource(new FileSystemResource("staging/billing-2023-01.csv"))
                .delimited()
                .names("dataYear", "dataMonth", "accountId", "phoneNumber", "dataUsage", "callDuration", "smsCount")
                .targetType(BillingData.class)
                .build();
    }

    @Bean
    JdbcBatchItemWriter<BillingData> billingDataItemWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<BillingData>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO BILLING_DATA values
                        (:dataYear, :dataMonth, :accountId, :phoneNumber, :dataUsage, :callDuration, :smsCount)
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    Step step2(JobRepository jobRepository,
               JdbcTransactionManager jdbcTransactionManager,
               ItemReader<BillingData> billingDataItemReader,
               ItemWriter<BillingData> billingDataItemWriter) {
        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(100, jdbcTransactionManager)
                .reader(billingDataItemReader)
                .writer(billingDataItemWriter)
                .build();
    }

    @Bean
    Job billingJob(JobRepository jobRepository, Step step1, Step step2) {
        return new JobBuilder("BillingJob", jobRepository)
                .start(step1)
                .next(step2)
                .build();
    }

}
