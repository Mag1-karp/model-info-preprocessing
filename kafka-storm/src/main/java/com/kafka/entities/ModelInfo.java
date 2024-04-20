package com.kafka.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelInfo {
    private String modelId;
    private String sha;
    private String lastModified;
    private List<String> tags;
    @JsonProperty("pipeline_tag")
    private String pipelineTag;
    private List<Sibling> siblings;
    @JsonProperty("private")
    private boolean privateModel;
    private String author;
    private Config config;
    @JsonProperty("securityStatus")
    private String securityStatus;
    private String _id;
    private String id;
    // @JsonProperty("cardData")
    // private CardData cardData;
    private int likes;
    private int downloads;
    @JsonProperty("library_name")
    private String libraryName;
    // getters and setters

    public String getModelId() {
        return modelId;
    }

    public String getSha() {
        return sha;
    }

    public String getLastModified() {
        return lastModified;
    }

    public List<String> getTags() {
        return tags;
    }

    public String getPipelineTag() {
        return pipelineTag;
    }

    public List<Sibling> getSiblings() {
        return siblings;
    }

    public boolean isPrivateModel() {
        return privateModel;
    }

    public String getAuthor() {
        return author;
    }

    public Config getConfig() {
        return config;
    }

    public String getSecurityStatus() {
        return securityStatus;
    }

    public String get_id() {
        return _id;
    }

    public String getId() {
        return id;
    }

//    public CardData getCardData() {
//        return cardData;
//    }

    public int getLikes() {
        return likes;
    }

    public int getDownloads() {
        return downloads;
    }

    public String getLibraryName() {
        return libraryName;
    }

    // setters
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public void setSha(String sha) {
        this.sha = sha;
    }

    public void setLastModified(String lastModified) {
        this.lastModified = lastModified;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public void setPipelineTag(String pipelineTag) {
        this.pipelineTag = pipelineTag;
    }

    public void setSiblings(List<Sibling> siblings) {
        this.siblings = siblings;
    }

    public void setPrivateModel(boolean privateModel) {
        this.privateModel = privateModel;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public void setSecurityStatus(String securityStatus) {
        this.securityStatus = securityStatus;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public void setId(String id) {
        this.id = id;
    }

//    public void setCardData(CardData cardData) {
//        this.cardData = cardData;
//    }

    public void setLikes(int likes) {
        this.likes = likes;
    }

    public void setDownloads(int downloads) {
        this.downloads = downloads;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }
}

