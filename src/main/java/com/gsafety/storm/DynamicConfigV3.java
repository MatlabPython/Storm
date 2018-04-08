package com.gsafety.storm;

import java.util.List;

/**
 * Author: huangll
 * Written on 17/8/31.
 */
public class DynamicConfigV3 {

  //动态配置还是静态配置
  private Boolean dynamic;

  //毛刺配置
  private Double burrMax;
  private Double burrMin;

  //监测值上限
  private Double sensorRangeMax;

  //监测值下限
  private Double sensorRangeMin;

  //平衡清零配置
  private Double zero;

  private List<AlarmConfig> alarmConfigs;


  //报警间隔 单位毫秒
  private Long AlarmInterval;

  public Double getBurrMax() {
    return burrMax;
  }

  public void setBurrMax(Double burrMax) {
    this.burrMax = burrMax;
  }

  public Double getBurrMin() {
    return burrMin;
  }

  public void setBurrMin(Double burrMin) {
    this.burrMin = burrMin;
  }

  public Double getZero() {
    return zero;
  }

  public void setZero(Double zero) {
    this.zero = zero;
  }


  public List<AlarmConfig> getAlarmConfigs() {
    return alarmConfigs;
  }

  public void setAlarmConfigs(List<AlarmConfig> alarmConfigs) {
    this.alarmConfigs = alarmConfigs;
  }

  public Boolean getDynamic() {
    return dynamic;
  }

  public void setDynamic(Boolean dynamic) {
    this.dynamic = dynamic;
  }

  public Double getSensorRangeMax() {
    return sensorRangeMax;
  }

  public void setSensorRangeMax(Double sensorRangeMax) {
    this.sensorRangeMax = sensorRangeMax;
  }

  public void setSensorRangeMin(Double sensorRangeMin) {
    this.sensorRangeMin = sensorRangeMin;
  }

  public Double getSensorRangeMin() {
    return sensorRangeMin;
  }

  public Long getAlarmInterval() {
    return AlarmInterval;
  }

  public void setAlarmInterval(Long alarmInterval) {
    AlarmInterval = alarmInterval;
  }

}
