package com.gsafety.storm;

import java.util.List;

/**
 * Created by hadoop on 2018/1/8.
 */
public class HeatRange {
    private Float DrainTheoreticalValue;
    private Float DrainActualValue;

    public HeatRange() {
    }

    public HeatRange(Float drainTheoreticalValue, Float drainActualValue) {
        DrainTheoreticalValue = drainTheoreticalValue;
        DrainActualValue = drainActualValue;
    }

    public Float getDrainTheoreticalValue() {
        return DrainTheoreticalValue;
    }

    public void setDrainTheoreticalValue(Float drainTheoreticalValue) {
        DrainTheoreticalValue = drainTheoreticalValue;
    }

    public Float getDrainActualValue() {
        return DrainActualValue;
    }

    public void setDrainActualValue(Float drainActualValue) {
        DrainActualValue = drainActualValue;
    }
}
