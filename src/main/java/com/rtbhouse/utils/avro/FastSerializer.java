package com.rtbhouse.utils.avro;

import java.io.IOException;

import org.apache.avro.io.Encoder;

public interface FastSerializer<Type> {

    void serialize(Type data, Encoder e) throws IOException;

}
