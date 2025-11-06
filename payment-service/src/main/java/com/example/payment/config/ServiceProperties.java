package com.example.payment.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "fail-rate")
@RefreshScope
public class ServiceProperties {

    private double payment;

    public double getPayment() { return payment; }
    public void setPayment(double payment) { this.payment = payment; }
}
