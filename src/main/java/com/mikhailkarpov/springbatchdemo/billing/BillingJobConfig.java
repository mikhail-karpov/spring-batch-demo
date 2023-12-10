package com.mikhailkarpov.springbatchdemo.billing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.DataClassRowMapper;
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
    FlatFileItemReader<BillingData> billingDataFileReader() {
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
               ItemReader<BillingData> billingDataFileReader,
               ItemWriter<BillingData> billingDataItemWriter) {
        return new StepBuilder("fileIngestion", jobRepository)
                .<BillingData, BillingData>chunk(100, jdbcTransactionManager)
                .reader(billingDataFileReader)
                .writer(billingDataItemWriter)
                .build();
    }

    @Bean
    JdbcCursorItemReader<BillingData>  billingDataJdbcReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<BillingData>()
                .name("billingDataJdbcReader")
                .dataSource(dataSource)
                .sql("SELECT * FROM BILLING_DATA")
                .rowMapper(new DataClassRowMapper<>(BillingData.class))
                .build();
    }

    @Bean
    FlatFileItemWriter<ReportingData> reportingDataFileWriter() {
        return new FlatFileItemWriterBuilder<ReportingData>()
                .name("reportingDataFileWriter")
                .resource(new FileSystemResource("staging/billing-report-2023-01.csv"))
                .delimited()
                .names("billingData.dataYear", "billingData.dataMonth", "billingData.accountId", "billingData.phoneNumber", "billingData.dataUsage", "billingData.callDuration", "billingData.smsCount", "billingTotal")
                .build();
    }

    @Bean
    Step step3(JobRepository jobRepository,
               JdbcTransactionManager jdbcTransactionManager,
               ItemReader<BillingData> billingDataJdbcReader,
               ItemWriter<ReportingData> reportingDataItemWriter) {
        return new StepBuilder("reportGeneration", jobRepository)
                .<BillingData, ReportingData>chunk(100, jdbcTransactionManager)
                .reader(billingDataJdbcReader)
                .processor(new BillingDataProcessor())
                .writer(reportingDataItemWriter)
                .build();
    }

    @Bean
    Job billingJob(JobRepository jobRepository, Step step1, Step step2, Step step3) {
        return new JobBuilder("BillingJob", jobRepository)
                .start(step1)
                .next(step2)
                .next(step3)
                .build();
    }

}
