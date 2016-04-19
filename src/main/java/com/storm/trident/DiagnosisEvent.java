package com.storm.trident;

/**
 * Created by zhangxiaolei05 on 2016/4/12.
 */
public class DiagnosisEvent {
    public double lat;
    public double lng;
    public long time;
    public String diagnosisCode;

    public DiagnosisEvent(double lat, double lng, long time, String diagnosisCode) {
        super();
        this.time = time;
        this.lat = lat;
        this.lng = lng;
        this.diagnosisCode = diagnosisCode;
    }
}
