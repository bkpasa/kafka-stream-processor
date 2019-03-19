package com.bkpasa.kafkastream;

public class MessageException extends Exception {

    private static final long serialVersionUID = -3290038828204366693L;

    public MessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
