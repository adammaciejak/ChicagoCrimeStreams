package com.chicago.crimes.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CrimeAggregate {
    @JsonProperty("yearMonth")
    private String yearMonth;

    @JsonProperty("primaryDescription")
    private String primaryDescription;

    @JsonProperty("district")
    private String district;

    @JsonProperty("totalCrimes")
    private long totalCrimes;

    @JsonProperty("arrestCount")
    private long arrestCount;

    @JsonProperty("domesticCount")
    private long domesticCount;

    @JsonProperty("fbiIndexCount")
    private long fbiIndexCount;

    // Konstruktory
    public CrimeAggregate() {}

    public CrimeAggregate(String yearMonth, String primaryDescription, String district) {
        this.yearMonth = yearMonth;
        this.primaryDescription = primaryDescription;
        this.district = district;
        this.totalCrimes = 0;
        this.arrestCount = 0;
        this.domesticCount = 0;
        this.fbiIndexCount = 0;
    }

    // Metoda do aktualizacji agregatu
    public CrimeAggregate update(CrimeRecord crime, boolean isFbiIndex) {
        this.totalCrimes++;
        if (crime.isArrest()) {
            this.arrestCount++;
        }
        if (crime.isDomestic()) {
            this.domesticCount++;
        }
        if (isFbiIndex) {
            this.fbiIndexCount++;
        }
        return this;
    }

    // Gettery i settery
    public String getYearMonth() { return yearMonth; }
    public void setYearMonth(String yearMonth) { this.yearMonth = yearMonth; }

    public String getPrimaryDescription() { return primaryDescription; }
    public void setPrimaryDescription(String primaryDescription) { this.primaryDescription = primaryDescription; }

    public String getDistrict() { return district; }
    public void setDistrict(String district) { this.district = district; }

    public long getTotalCrimes() { return totalCrimes; }
    public void setTotalCrimes(long totalCrimes) { this.totalCrimes = totalCrimes; }

    public long getArrestCount() { return arrestCount; }
    public void setArrestCount(long arrestCount) { this.arrestCount = arrestCount; }

    public long getDomesticCount() { return domesticCount; }
    public void setDomesticCount(long domesticCount) { this.domesticCount = domesticCount; }

    public long getFbiIndexCount() { return fbiIndexCount; }
    public void setFbiIndexCount(long fbiIndexCount) { this.fbiIndexCount = fbiIndexCount; }
}
