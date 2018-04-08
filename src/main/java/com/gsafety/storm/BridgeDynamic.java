package com.gsafety.storm;

/**
 * Created by hadoop on 2018/1/5.
 */
public class BridgeDynamic {
    private Double burrMin;
    private Double burrMax;
    private Double zero;
    private Double alarmFirstLevelUp;
    private Double alarmFirstLevelDown;
    private Double alarmSecondLevelUp;
    private Double alarmSecondLevelDown;
    private Double alarmThirdLevelUp;
    private Double alarmThirdLevelDown;

    public BridgeDynamic() {
    }

    public BridgeDynamic(Double burrMin, Double burrMax, Double zero, Double alarmFirstLevelUp, Double alarmFirstLevelDown, Double alarmSecondLevelUp, Double alarmSecondLevelDown, Double alarmThirdLevelUp, Double alarmThirdLevelDown) {
        this.burrMin = burrMin;
        this.burrMax = burrMax;
        this.zero = zero;
        this.alarmFirstLevelUp = alarmFirstLevelUp;
        this.alarmFirstLevelDown = alarmFirstLevelDown;
        this.alarmSecondLevelUp = alarmSecondLevelUp;
        this.alarmSecondLevelDown = alarmSecondLevelDown;
        this.alarmThirdLevelUp = alarmThirdLevelUp;
        this.alarmThirdLevelDown = alarmThirdLevelDown;
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

    public Double getZero() {
        return zero;
    }

    public void setZero(Double zero) {
        this.zero = zero;
    }

    public Double getAlarmFirstLevelUp() {
        return alarmFirstLevelUp;
    }

    public void setAlarmFirstLevelUp(Double alarmFirstLevelUp) {
        this.alarmFirstLevelUp = alarmFirstLevelUp;
    }

    public Double getAlarmFirstLevelDown() {
        return alarmFirstLevelDown;
    }

    public void setAlarmFirstLevelDown(Double alarmFirstLevelDown) {
        this.alarmFirstLevelDown = alarmFirstLevelDown;
    }

    public Double getAlarmSecondLevelUp() {
        return alarmSecondLevelUp;
    }

    public void setAlarmSecondLevelUp(Double alarmSecondLevelUp) {
        this.alarmSecondLevelUp = alarmSecondLevelUp;
    }

    public Double getAlarmSecondLevelDown() {
        return alarmSecondLevelDown;
    }

    public void setAlarmSecondLevelDown(Double alarmSecondLevelDown) {
        this.alarmSecondLevelDown = alarmSecondLevelDown;
    }

    public Double getAlarmThirdLevelUp() {
        return alarmThirdLevelUp;
    }

    public void setAlarmThirdLevelUp(Double alarmThirdLevelUp) {
        this.alarmThirdLevelUp = alarmThirdLevelUp;
    }

    public Double getAlarmThirdLevelDown() {
        return alarmThirdLevelDown;
    }

    public void setAlarmThirdLevelDown(Double alarmThirdLevelDown) {
        this.alarmThirdLevelDown = alarmThirdLevelDown;
    }
}
