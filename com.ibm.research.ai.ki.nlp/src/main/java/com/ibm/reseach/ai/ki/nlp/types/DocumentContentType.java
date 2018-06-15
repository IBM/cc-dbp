package com.ibm.reseach.ai.ki.nlp.types;

import com.ibm.reseach.ai.ki.nlp.*;

import com.fasterxml.jackson.annotation.*;

public class DocumentContentType implements DocumentStructure {

    private static final long serialVersionUID = 1L;
    public String contentType;
    @JsonCreator
    public DocumentContentType(@JsonProperty("contentType") String contentType) {
        this.contentType = contentType;
    }

}
