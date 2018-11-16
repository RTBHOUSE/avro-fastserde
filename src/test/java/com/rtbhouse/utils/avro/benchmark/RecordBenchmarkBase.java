package com.rtbhouse.utils.avro.benchmark;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.openjdk.jmh.annotations.Setup;

import com.rtbhouse.utils.avro.FastGenericDatumReader;
import com.rtbhouse.utils.avro.FastGenericDatumWriter;
import com.rtbhouse.utils.avro.FastSerdeCache;
import com.rtbhouse.utils.avro.FastSpecificDatumReader;
import com.rtbhouse.utils.avro.FastSpecificDatumWriter;

public abstract class RecordBenchmarkBase<T extends SpecificRecord> {
    private static final FastSerdeCache cache = new FastSerdeCache(Runnable::run);

    protected Schema specificRecordSchema;

    private List<GenericData.Record> genericRecords = new ArrayList<>();
    private List<T> specificRecords = new ArrayList<>();
    private List<byte[]> recordBytes = new ArrayList<>();

    private FastGenericDatumReader<GenericData.Record> fastGenericDatumReader;
    private FastGenericDatumWriter<GenericData.Record> fastGenericDatumWriter;
    private GenericDatumReader<GenericData.Record> genericDatumReader;
    private GenericDatumWriter<GenericData.Record> genericDatumWriter;

    private FastSpecificDatumReader<T> fastSpecificDatumReader;
    private FastSpecificDatumWriter<T> fastSpecificDatumWriter;
    private GenericDatumReader<T> specificDatumReader;
    private GenericDatumWriter<T> specificDatumWriter;

    @Setup
    public void init() throws Exception {
        final GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(specificRecordSchema);
        for (int i = 0; i < 1000; i++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);

            genericRecords.add(FastSerdeBenchmarkSupport.generateRandomRecordData(specificRecordSchema));
            specificRecords
                    .add(FastSerdeBenchmarkSupport.toSpecificRecord(genericRecords.get(genericRecords.size() - 1)));

            datumWriter.write(genericRecords.get(genericRecords.size() - 1), encoder);
            encoder.flush();

            recordBytes.add(baos.toByteArray());
        }
        fastGenericDatumReader = new FastGenericDatumReader<>(
                specificRecordSchema, cache);
        fastGenericDatumWriter = new FastGenericDatumWriter<>(specificRecordSchema, cache);

        genericDatumReader = new GenericDatumReader<>(specificRecordSchema);
        genericDatumWriter = new GenericDatumWriter<>(specificRecordSchema);

        fastSpecificDatumReader = new FastSpecificDatumReader<>(
                specificRecordSchema, cache);
        fastSpecificDatumWriter = new FastSpecificDatumWriter<>(specificRecordSchema, cache);

        specificDatumReader = new SpecificDatumReader<>(specificRecordSchema);
        specificDatumWriter = new SpecificDatumWriter<>(specificRecordSchema);
    }

    public void fastGenericDatumReader() throws Exception {
        for (byte[] bytes : recordBytes) {
            fastGenericDatumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }
    }

    public void genericDatumReader() throws Exception {
        for (byte[] bytes : recordBytes) {
            genericDatumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }
    }

    public void fastGenericDatumWriter() throws Exception {
        for (GenericData.Record record : genericRecords) {
            fastGenericDatumWriter.write(record, EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null));
        }
    }

    public void genericDatumWriter() throws Exception {
        for (GenericData.Record record : genericRecords) {
            genericDatumWriter.write(record, EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null));
        }
    }

    public void fastSpecificDatumReader() throws Exception {
        for (byte[] bytes : recordBytes) {
            fastSpecificDatumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }
    }

    public void specificDatumReader() throws Exception {
        for (byte[] bytes : recordBytes) {
            specificDatumReader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
        }
    }

    public void fastSpecificDatumWriter() throws Exception {
        for (T record : specificRecords) {
            fastSpecificDatumWriter.write(record,
                    EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null));
        }
    }

    public void specificDatumWriter() throws Exception {
        for (T record : specificRecords) {
            specificDatumWriter.write(record, EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null));
        }
    }

}
