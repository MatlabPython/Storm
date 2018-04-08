package com.gsafety.storm;

/**
 * Created by hadoop on 2018/1/8.
 */
public class HeatRangInner {
    private Float DrainTheoreticalValue;
    private Float DrainActualValue;

    public HeatRangInner() {
    }

    public HeatRangInner(Float drainTheoreticalValue, Float drainActualValue) {
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
