package com.chicago.crimes.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CrimeRecord {
    @JsonProperty("ID")
    private String id;

    @JsonProperty("Date")
    private String date;

    @JsonProperty("IUCR")
    private String iucr;

    @JsonProperty("Arrest")
    private boolean arrest;

    @JsonProperty("Domestic")
    private boolean domestic;

    @JsonProperty("District")
    private String district;

    @JsonProperty("ComArea")
    private String comArea;

    @JsonProperty("Latitude")
    private Double latitude;

    @JsonProperty("Longitude")
    private Double longitude;

    // Konstruktory
    public CrimeRecord() {}

    public CrimeRecord(String id, String date, String iucr, boolean arrest,
                       boolean domestic, String district, String comArea,
                       Double latitude, Double longitude) {
        this.id = id;
        this.date = date;
        this.iucr = iucr;
        this.arrest = arrest;
        this.domestic = domestic;
        this.district = district;
        this.comArea = comArea;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    // Gettery i settery
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getDate() { return date; }
    public void setDate(String date) { this.date = date; }

    public String getIucr() { return iucr; }
    public void setIucr(String iucr) { this.iucr = iucr; }

    public boolean isArrest() { return arrest; }
    public void setArrest(boolean arrest) { this.arrest = arrest; }

    public boolean isDomestic() { return domestic; }
    public void setDomestic(boolean domestic) { this.domestic = domestic; }

    public String getDistrict() { return district; }
    public void setDistrict(String district) { this.district = district; }

    public String getComArea() { return comArea; }
    public void setComArea(String comArea) { this.comArea = comArea; }

    public Double getLatitude() { return latitude; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }

    public Double getLongitude() { return longitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }

    @JsonIgnore
    public LocalDateTime getDateTime() {
        try {
            return LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }
    @JsonIgnore
    public String getYearMonth() {
        return getDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM"));
    }
}
