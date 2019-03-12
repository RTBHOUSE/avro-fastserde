package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createPrimitiveUnionFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createRecord;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.emptyTestRecord;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.serializeGeneric;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.serializeSpecific;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rtbhouse.utils.generated.avro.TestEnum;
import com.rtbhouse.utils.generated.avro.TestRecord;


public class FastDatumReaderTest {

    private FastSerdeCache cache;

    @Before
    public void before() {
        cache = new FastSerdeCache(Runnable::run);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateSpecificDatumReader() throws IOException {
        // given
        FastSpecificDatumReader<TestRecord> fastSpecificDatumReader = new FastSpecificDatumReader<>(
                TestRecord.getClassSchema(), cache);

        TestRecord testRecord = emptyTestRecord();
        testRecord.put("testEnum", TestEnum.A);

        // when
        fastSpecificDatumReader.read(null, serializeSpecific(testRecord));

        // then
        FastDeserializer<TestRecord> fastSpecificDeserializer =
                (FastDeserializer<TestRecord>) cache.getFastSpecificDeserializer(
                        TestRecord.getClassSchema(), TestRecord.getClassSchema());

        fastSpecificDeserializer =
                (FastDeserializer<TestRecord>) cache.getFastSpecificDeserializer(
                        TestRecord.getClassSchema(), TestRecord.getClassSchema());

        Assert.assertNotNull(fastSpecificDeserializer);
        Assert.assertNotEquals(2, fastSpecificDeserializer.getClass().getDeclaredMethods().length);
        Assert.assertEquals(
                TestEnum.A,
                fastSpecificDatumReader.read(null,
                        FastSerdeTestsSupport.serializeSpecific(testRecord)).getTestEnum());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotCreateSpecificDatumReader() throws IOException {
        // given
        Schema faultySchema = createRecord("FaultySchema");
        FastSpecificDatumReader<TestRecord> fastSpecificDatumReader = new FastSpecificDatumReader<>(
                TestRecord.getClassSchema(), faultySchema, cache);

        TestRecord testRecord = emptyTestRecord();
        testRecord.put("testEnum", TestEnum.A);

        // when
        fastSpecificDatumReader.read(null, FastSerdeTestsSupport.serializeSpecific(testRecord));

        // then
        FastDeserializer<TestRecord> fastSpecificDeserializer =
                (FastDeserializer<TestRecord>) cache.getFastSpecificDeserializer(
                        TestRecord.getClassSchema(), faultySchema);

        fastSpecificDeserializer =
                (FastDeserializer<TestRecord>) cache.getFastSpecificDeserializer(
                        TestRecord.getClassSchema(), faultySchema);

        Assert.assertNotNull(fastSpecificDeserializer);
        Assert.assertEquals(2, fastSpecificDeserializer.getClass().getDeclaredMethods().length);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateGenericDatumReader() throws IOException, InterruptedException {
        Schema recordSchema = createRecord("TestSchema",
            createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
        FastGenericDatumReader<GenericRecord> fastGenericDatumReader = new FastGenericDatumReader<>(
                recordSchema, cache);

        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
        recordBuilder.set("test", "test");

        // when
        fastGenericDatumReader.read(null, serializeGeneric(recordBuilder.build()));

        // then
        FastDeserializer<GenericRecord> fastGenericDeserializer =
                (FastDeserializer<GenericRecord>) cache.getFastGenericDeserializer(
                        recordSchema, recordSchema);

        fastGenericDeserializer =
                (FastDeserializer<GenericRecord>) cache.getFastGenericDeserializer(
                        recordSchema, recordSchema);

        Assert.assertNotNull(fastGenericDeserializer);
        Assert.assertNotEquals(2, fastGenericDeserializer.getClass().getDeclaredMethods().length);
        Assert.assertEquals(
                "test",
                fastGenericDatumReader.read(null, serializeGeneric(recordBuilder.build())).get("test"));
    }
}
