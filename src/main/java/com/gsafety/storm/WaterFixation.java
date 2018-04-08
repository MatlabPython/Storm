package com.gsafety.storm;

/**
 * Created by hadoop on 2018/1/10.
 */
public class WaterFixation {
    private Double burrMin;
    private Double burrMax;
    private Double sensorRangeMax;
    private Double sensorRangeMin;

    public WaterFixation() {
    }

    public WaterFixation(Double burrMin, Double burrMax, Double sensorRangeMax, Double sensorRangeMin) {
        this.burrMin = burrMin;
        this.burrMax = burrMax;
        this.sensorRangeMax = sensorRangeMax;
        this.sensorRangeMin = sensorRangeMin;
    }

    public Double getBurrMin() {
        return burrMin;
    }

    public void setBurrMin(Double burrMin) {
        this.burrMin = burrMin;
    }

    public Double getBurrMax() {
        return burrMax;
    }

    public void setBurrMax(Double burrMax) {
        this.burrMax = burrMax;
    }

    public Double getSensorRangeMax() {
        return sensorRangeMax;
    }

    public void setSensorRangeMax(Double sensorRangeMax) {
        this.sensorRangeMax = sensorRangeMax;
    }

    public Double getSensorRangeMin() {
        return sensorRangeMin;
    }

    public void setSensorRangeMin(Double sensorRangeMin) {
        this.sensorRangeMin = sensorRangeMin;
    }
}
