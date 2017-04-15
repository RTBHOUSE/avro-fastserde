package com.rtbhouse.utils.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

/**
 * Generic {@link DatumReader} backed by generated deserialization code.
 */
public class FastGenericDatumReader<T> implements DatumReader<T> {

    private Schema writerSchema;
    private Schema readerSchema;
    private FastSerdeCache cache;

    public FastGenericDatumReader(Schema schema) {
        this(schema, schema);
    }

    public FastGenericDatumReader(Schema writerSchema, Schema readerSchema) {
       this(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance());
    }

    public FastGenericDatumReader(Schema schema, FastSerdeCache cache) {
        this(schema, schema, cache);
    }

    public FastGenericDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache) {
        this.writerSchema = writerSchema;
        this.readerSchema = readerSchema;
        this.cache = cache != null ? cache : FastSerdeCache.getDefaultInstance();
    }


    @Override
    public void setSchema(Schema schema) {
        if (writerSchema == null) {
            writerSchema = schema;
        }

        if (readerSchema == null) {
            readerSchema = writerSchema;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T read(T reuse, Decoder in) throws IOException {
        FastDeserializer<T> fastDeserializer = (FastDeserializer<T>) cache
                .getFastGenericDeserializer(writerSchema, readerSchema);

        return fastDeserializer.deserialize(in);
    }

}
