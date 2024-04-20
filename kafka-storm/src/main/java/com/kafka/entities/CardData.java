package com.kafka.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CardData {
    private List<String> tags;
    private String language;
    private String license;
    @JsonProperty("library_name")
    private String libraryName;
    private List<String> datasets;
    private List<String> metrics;
    private String thumbnail;
    private boolean inference;
    // getters and setters

    public List<String> getTags() {
        return tags;
    }

    public String getLanguage() {
        return language;
    }

    public String getLicense() {
        return license;
    }

    public List<String> getDatasets() {
        return datasets;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public boolean isInference() {
        return inference;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    public void setDatasets(List<String> datasets) {
        this.datasets = datasets;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    public void setInference(boolean inference) {
        this.inference = inference;
    }
}
