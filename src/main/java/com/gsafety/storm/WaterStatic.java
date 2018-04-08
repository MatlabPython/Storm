package com.gsafety.storm;

import java.util.List;

/**
 * Created by hadoop on 2018/1/10.
 */
public class WaterStatic {
    private List<ThresholdValueCommon> waterStaticValue;

    public WaterStatic() {
    }

    public WaterStatic(List<ThresholdValueCommon> waterStaticValue) {
        this.waterStaticValue = waterStaticValue;
    }

    public List<ThresholdValueCommon> getWaterStaticValue() {
        return waterStaticValue;
    }

    public void setWaterStaticValue(List<ThresholdValueCommon> waterStaticValue) {
        this.waterStaticValue = waterStaticValue;
    }
}
