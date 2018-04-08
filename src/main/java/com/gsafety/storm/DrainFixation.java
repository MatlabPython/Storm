package com.gsafety.storm;

/**
 * @Author: yifeng G
 * @Date: Create in 16:19 2018/1/5 2018
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class DrainFixation {
    //阈值告警一级上限
    private Double alarmFirstLevelUp;
    //阈值告警一级下限
    private Double alarmFirstLevelDown;
    //阈值告警二级上限
    private Double alarmSecondLevelUp;
    //阈值告警二级下限
    private Double alarmSecondLevelDown;
    //阈值告警三级上限
    private Double alarmThirdLevelUp;
    //阈值告警三级下限
    private Double alarmThirdLevelDown;

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
