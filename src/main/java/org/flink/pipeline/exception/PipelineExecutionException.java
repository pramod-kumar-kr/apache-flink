package org.flink.pipeline.exception;

public class PipelineExecutionException extends RuntimeException {
    public PipelineExecutionException() {
        super();
    }

    public PipelineExecutionException(String message) {
        super(message);
    }

    public PipelineExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineExecutionException(Throwable cause) {
        super(cause);
    }
}
