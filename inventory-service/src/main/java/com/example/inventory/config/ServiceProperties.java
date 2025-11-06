package com.example.inventory.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "fail-rate")
@RefreshScope
public class ServiceProperties {

    private double inventory;

    public double getInventory() { return inventory; }
    public void setInventory(double inventory) { this.inventory = inventory; }
}
