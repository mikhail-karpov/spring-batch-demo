package com.mikhailkarpov.springbatchdemo.billing;

import org.springframework.batch.item.ItemProcessor;

public class BillingDataProcessor implements ItemProcessor<BillingData, ReportingData> {

    private double dataPricing = 0.01;
    private double callPricing = 0.5;
    private double smsPricing = 0.1;
    private double spendingThreshold = 150.0;

    @Override
    public ReportingData process(BillingData billingData) {
        double billingTotal = dataPricing * billingData.dataUsage() +
                callPricing * billingData.callDuration() +
                smsPricing * billingData.smsCount();
        if (billingTotal < spendingThreshold) {
            return null;
        }
        return new ReportingData(billingData, billingTotal);
    }
}
