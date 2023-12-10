package com.mikhailkarpov.springbatchdemo.billing;

import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BillingJobConfig {

    @Bean
    BillingJob billingJob(JobRepository jobRepository) {
        return new BillingJob(jobRepository);
    }

}
