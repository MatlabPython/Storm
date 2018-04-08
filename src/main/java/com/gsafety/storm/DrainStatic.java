package com.gsafety.storm;

import java.util.List;

/**
 * Created by hadoop on 2018/1/8.
 */
public class DrainStatic {
    private List<ThresholdValueCommon> drainStaticValue;
    public DrainStatic() {
    }

    public DrainStatic(List<ThresholdValueCommon> drainStaticValue) {
        this.drainStaticValue = drainStaticValue;
    }

    public List<ThresholdValueCommon> getDrainStaticValue() {
        return drainStaticValue;
    }

    public void setDrainStaticValue(List<ThresholdValueCommon> drainStaticValue) {
        this.drainStaticValue = drainStaticValue;
    }
}
