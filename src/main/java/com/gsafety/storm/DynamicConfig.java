package com.gsafety.storm;

/**
 * Author: huangll
 * Written on 17/8/31.
 */
public class DynamicConfig {

  private Double burr;

  private Double zero;

  private Double alarmFirstLevelUp;

  private Double alarmFirstLevelDown;

  private Double alarmSecondLevelUp;

  private Double alarmSecondLevelDown;

  private Double alarmThirdLevelUp;

  private Double alarmThirdLevelDown;

  public Double getBurr() {
    return burr;
  }

  public void setBurr(Double burr) {
    this.burr = burr;
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

  @Override
  public String toString() {
    return "DynamicConfig{" +
        "burr=" + burr +
        ", zero=" + zero +
        ", alarmFirstLevelUp=" + alarmFirstLevelUp +
        ", alarmFirstLevelDown=" + alarmFirstLevelDown +
        ", alarmSecondLevelUp=" + alarmSecondLevelUp +
        ", alarmSecondLevelDown=" + alarmSecondLevelDown +
        ", alarmThirdLevelUp=" + alarmThirdLevelUp +
        ", alarmThirdLevelDown=" + alarmThirdLevelDown +
        '}';
  }
}
