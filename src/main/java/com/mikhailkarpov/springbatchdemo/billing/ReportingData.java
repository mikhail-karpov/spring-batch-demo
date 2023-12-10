package com.mikhailkarpov.springbatchdemo.billing;

public record ReportingData(
        BillingData billingData,
        double billingTotal) {
}
