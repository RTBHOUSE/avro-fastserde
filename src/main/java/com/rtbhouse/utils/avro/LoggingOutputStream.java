package com.rtbhouse.utils.avro;

import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

final class LoggingOutputStream extends OutputStream {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    private final Consumer<String> loggingMethod;

    private LoggingOutputStream(Consumer<String> loggingMethod) {
        this.loggingMethod = loggingMethod;
    }

    @Override
    public void write(int b) {
        if (b == '\n') {
            String line = baos.toString();
            baos.reset();
            loggingMethod.accept(line);
        } else {
            baos.write(b);
        }
    }

    static LoggingOutputStream infoLoggingStream(Logger logger) {
        return new LoggingOutputStream(logger::info);
    }

    static LoggingOutputStream errorLoggingStream(Logger logger) {
        return new LoggingOutputStream(logger::error);
    }

}