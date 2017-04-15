package com.rtbhouse.utils.avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

/**
 * Specific {@link DatumReader} backed by generated deserialization code.
 */
public class FastSpecificDatumReader<T> implements DatumReader<T> {

    private Schema writerSchema;
    private Schema readerSchema;
    private FastSerdeCache cache;

    public FastSpecificDatumReader(Schema schema) {
        this(schema, schema);
    }

    public FastSpecificDatumReader(Schema writerSchema, Schema readerSchema) {
        this(writerSchema, readerSchema, FastSerdeCache.getDefaultInstance());
    }

    public FastSpecificDatumReader(Schema schema, FastSerdeCache cache) {
        this(schema, schema, cache);
    }

    public FastSpecificDatumReader(Schema writerSchema, Schema readerSchema, FastSerdeCache cache) {
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
                .getFastSpecificDeserializer(writerSchema, readerSchema);

        return fastDeserializer.deserialize(in);
    }
}
