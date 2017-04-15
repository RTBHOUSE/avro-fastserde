package com.rtbhouse.utils.avro;

import java.io.File;

import org.apache.avro.Schema;

public final class FastSpecificDeserializerGenerator<T> extends FastDeserializerGenerator<T> {

    FastSpecificDeserializerGenerator(Schema writer, Schema reader, File destination, ClassLoader classLoader,
        String compileClassPath) {
        super(false, writer, reader, destination, classLoader, compileClassPath);
    }
}
