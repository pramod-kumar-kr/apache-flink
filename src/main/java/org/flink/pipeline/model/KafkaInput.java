package org.flink.pipeline.model;

import org.apache.kafka.common.header.Headers;

public class KafkaInput {
    Headers headers;
    String key;
    String value;
    int partition;
    long offset;

    public KafkaInput(Headers headers, String key, String value, int partition, long offset) {
        this.headers = headers;
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.offset = offset;
    }

    public Headers getHeaders() {
        return headers;
    }

    public void setHeaders(Headers headers) {
        this.headers = headers;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KafkaInput{" +
            "headers=" + headers +
            ", key='" + key + '\'' +
            ", value='" + value + '\'' +
            ", partition=" + partition +
            ", offset=" + offset +
            '}';
    }
}
