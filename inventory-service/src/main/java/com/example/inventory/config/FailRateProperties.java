package com.example.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "fail-rate")
@RefreshScope
public class FailRateProperties {

    private double inventory;
    private double payment;

    public double getInventory() { return inventory; }
    public void setInventory(double inventory) { this.inventory = inventory; }

    public double getPayment() { return payment; }
    public void setPayment(double payment) { this.payment = payment; }
}
