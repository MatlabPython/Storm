package com.gsafety.storm;

/**
 * @Author: yifeng G
 * @Date: Create in 11:07 2018/5/31 2018
 * @Description:
 * @Modified By:
 * @Vsersion:
 */
public class BridgeEquConf {
    private String bridgename;
    private String monitoringname;
    private String terminal;
    private String sensor;

    public BridgeEquConf() {

    }

    public BridgeEquConf(String bridgename, String monitoringname, String terminal, String sensor) {
        this.bridgename = bridgename;
        this.monitoringname = monitoringname;
        this.terminal = terminal;
        this.sensor = sensor;
    }

    public String getBridgename() {
        return bridgename;
    }

    public void setBridgename(String bridgename) {
        this.bridgename = bridgename;
    }

    public String getMonitoringname() {
        return monitoringname;
    }

    public void setMonitoringname(String monitoringname) {
        this.monitoringname = monitoringname;
    }

    public String getTerminal() {
        return terminal;
    }

    public void setTerminal(String terminal) {
        this.terminal = terminal;
    }

    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }
}
