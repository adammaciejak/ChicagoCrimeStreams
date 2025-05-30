package com.chicago.crimes.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AnomalyAlert {
    @JsonProperty("windowStart")
    private String windowStart;

    @JsonProperty("windowEnd")
    private String windowEnd;

    @JsonProperty("district")
    private String district;

    @JsonProperty("fbiIndexCrimes")
    private long fbiIndexCrimes;

    @JsonProperty("totalCrimes")
    private long totalCrimes;

    @JsonProperty("fbiPercentage")
    private double fbiPercentage;

    // Konstruktory
    public AnomalyAlert() {}

    public AnomalyAlert(String windowStart, String windowEnd, String district,
                        long fbiIndexCrimes, long totalCrimes) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.district = district;
        this.fbiIndexCrimes = fbiIndexCrimes;
        this.totalCrimes = totalCrimes;
        this.fbiPercentage = totalCrimes > 0 ? (double) fbiIndexCrimes / totalCrimes * 100 : 0;
    }

    // Gettery i settery
    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String windowStart) { this.windowStart = windowStart; }

    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String windowEnd) { this.windowEnd = windowEnd; }

    public String getDistrict() { return district; }
    public void setDistrict(String district) { this.district = district; }

    public long getFbiIndexCrimes() { return fbiIndexCrimes; }
    public void setFbiIndexCrimes(long fbiIndexCrimes) { this.fbiIndexCrimes = fbiIndexCrimes; }

    public long getTotalCrimes() { return totalCrimes; }
    public void setTotalCrimes(long totalCrimes) { this.totalCrimes = totalCrimes; }

    public double getFbiPercentage() { return fbiPercentage; }
    public void setFbiPercentage(double fbiPercentage) { this.fbiPercentage = fbiPercentage; }
}
