package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createPrimitiveUnionFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createRecord;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.emptyTestRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rtbhouse.utils.generated.avro.TestEnum;
import com.rtbhouse.utils.generated.avro.TestRecord;

public class FastDatumWriterTest {

    private FastSerdeCache cache;

    @Before
    public void before() {
        cache = new FastSerdeCache(Runnable::run);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateSpecificDatumWriter() throws IOException, InterruptedException {
        // given
        FastSpecificDatumWriter<TestRecord> fastSpecificDatumWriter = new FastSpecificDatumWriter<>(
                TestRecord.getClassSchema(), cache);

        TestRecord testRecord = emptyTestRecord();
        testRecord.put("testEnum", TestEnum.A);

        // when
        fastSpecificDatumWriter.write(testRecord,
                EncoderFactory.get().directBinaryEncoder(new ByteArrayOutputStream(), null));

        // then
        FastSerializer<TestRecord> fastSpecificSerializer = (FastSerializer<TestRecord>) cache
                .getFastSpecificSerializer(TestRecord.getClassSchema());

        fastSpecificSerializer = (FastSerializer<TestRecord>) cache
                .getFastSpecificSerializer(TestRecord.getClassSchema());

        Assert.assertNotNull(fastSpecificSerializer);
        Assert.assertNotEquals(2, fastSpecificSerializer.getClass().getDeclaredMethods().length);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateGenericDatumReader() throws IOException, InterruptedException {
        Schema recordSchema = createRecord("TestSchema",
                createPrimitiveUnionFieldSchema("test", Schema.Type.STRING));
        FastGenericDatumWriter<GenericRecord> fastGenericDatumReader = new FastGenericDatumWriter<>(
                recordSchema, cache);

        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
        recordBuilder.set("test", "test");

        // when
        fastGenericDatumReader.write(recordBuilder.build(),
                EncoderFactory.get().directBinaryEncoder(new ByteArrayOutputStream(), null));

        // then
        FastSerializer<GenericRecord> fastGenericSerializer = (FastSerializer<GenericRecord>) cache
                .getFastGenericSerializer(recordSchema);

        fastGenericSerializer = (FastSerializer<GenericRecord>) cache.getFastGenericSerializer(recordSchema);

        Assert.assertNotNull(fastGenericSerializer);
        Assert.assertNotEquals(2, fastGenericSerializer.getClass().getDeclaredMethods().length);
    }
}
