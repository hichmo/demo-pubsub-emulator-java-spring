package com.hmo.pubsub.producer.domain.model;

public class Message {

    public Message() {
    }

    private String content;

    public Message(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
