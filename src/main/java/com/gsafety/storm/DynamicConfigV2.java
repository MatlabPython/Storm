package com.gsafety.storm;

import java.util.List;

/**
 * Author: huangll
 * Written on 17/8/31.
 */
public class DynamicConfigV2 {

  //动态配置还是静态配置
  private Boolean dynamic;

  //毛刺配置
  private Double burr;
  //平衡清零配置
  private Double zero;

  private List<AlarmConfig> alarmConfigs;

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
}
