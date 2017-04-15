package com.rtbhouse.utils.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

/**
 * Generic {@link DatumWriter} backed by generated serialization code.
 */
public class FastGenericDatumWriter<T> implements DatumWriter<T> {

    private Schema writerSchema;
    private FastSerdeCache cache;

    public FastGenericDatumWriter(Schema schema) {
        this(schema, FastSerdeCache.getDefaultInstance());
    }

    public FastGenericDatumWriter(Schema schema, FastSerdeCache cache) {
        this.writerSchema = schema;
        this.cache = cache != null ? cache : FastSerdeCache.getDefaultInstance();
    }

    @Override
    public void setSchema(Schema schema) {
        writerSchema = schema;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(T data, Encoder out) throws IOException {
        FastSerializer<T> fastSerializer = (FastSerializer<T>) cache
            .getFastGenericSerializer(writerSchema);

        fastSerializer.serialize(data, out);
    }
}
