package com.gsafety.storm;

import java.util.List;

/**
 * Created by hadoop on 2018/1/8.
 */
public class HeatStatic {
    private List<ThresholdValueCommon> heatStaticValue;

    public HeatStatic() {
    }

    public HeatStatic(List<ThresholdValueCommon> heatStaticValue) {
        this.heatStaticValue = heatStaticValue;
    }

    public List<ThresholdValueCommon> getHeatStaticValue() {
        return heatStaticValue;
    }

    public void setHeatStaticValue(List<ThresholdValueCommon> heatStaticValue) {
        this.heatStaticValue = heatStaticValue;
    }
}
