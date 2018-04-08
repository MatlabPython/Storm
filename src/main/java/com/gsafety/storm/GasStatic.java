package com.gsafety.storm;

import java.util.List;

/**
 * Created by hadoop on 2018/1/8.
 */
public class GasStatic {
    private List<ThresholdValueCommon> gasStaticValue;

    public GasStatic() {
    }

    public GasStatic(List<ThresholdValueCommon> gasStaticValue) {
        this.gasStaticValue = gasStaticValue;
    }

    public List<ThresholdValueCommon> getGasStaticValue() {
        return gasStaticValue;
    }

    public void setGasStaticValue(List<ThresholdValueCommon> gasStaticValue) {
        this.gasStaticValue = gasStaticValue;
    }
}
