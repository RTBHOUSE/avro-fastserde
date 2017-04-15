package com.rtbhouse.utils.avro;

import java.io.IOException;

import org.apache.avro.io.Decoder;

public interface FastDeserializer<Type> {

    Type deserialize(Decoder d) throws IOException;

}
