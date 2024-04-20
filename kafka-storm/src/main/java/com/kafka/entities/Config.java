package com.kafka.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Config {
    private List<String> architectures;
    @JsonProperty("model_type")
    private String modelType;
    // getters and setters

    public List<String> getArchitectures() {
        return architectures;
    }

    public String getModelType() {
        return modelType;
    }

    public void setArchitectures(List<String> architectures) {
        this.architectures = architectures;
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }
}
