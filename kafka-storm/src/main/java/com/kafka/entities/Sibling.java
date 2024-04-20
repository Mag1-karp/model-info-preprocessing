package com.kafka.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Sibling {
    private String rfilename;
    private String size;
    private String blob_id;
    private String lfs;
    // getters and setters

    public String getRfilename() {
        return rfilename;
    }

    public String getSize() {
        return size;
    }

    public String getBlob_id() {
        return blob_id;
    }

    public String getLfs() {
        return lfs;
    }

    public void setRfilename(String rfilename) {
        this.rfilename = rfilename;
    }

    public void setSize(String size) {
        this.size = size;
    }

    public void setBlob_id(String blob_id) {
        this.blob_id = blob_id;
    }

    public void setLfs(String lfs) {
        this.lfs = lfs;
    }
}
