package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createArrayFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createEnumSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createField;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createFixedSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createMapFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createPrimitiveUnionFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createRecord;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionField;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.deserializeGeneric;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FastGenericSerializerGeneratorTest {

    private File tempDir;
    private ClassLoader classLoader;

    @Before
    public void prepare() throws Exception {
        Path tempPath = Files.createTempDirectory("generated");
        tempDir = tempPath.toFile();

        classLoader = URLClassLoader.newInstance(new URL[] { tempDir.toURI().toURL() },
                FastGenericSerializerGeneratorTest.class.getClassLoader());
    }

    @Test
    public void shouldWritePrimitives() {
        // given
        Schema recordSchema = createRecord("testRecord",
                createField("testInt", Schema.create(Schema.Type.INT)),
                createPrimitiveUnionFieldSchema("testIntUnion", Schema.Type.INT),
                createField("testString", Schema.create(Schema.Type.STRING)),
                createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING),
                createField("testLong", Schema.create(Schema.Type.LONG)),
                createPrimitiveUnionFieldSchema("testLongUnion", Schema.Type.LONG),
                createField("testDouble", Schema.create(Schema.Type.DOUBLE)),
                createPrimitiveUnionFieldSchema("testDoubleUnion", Schema.Type.DOUBLE),
                createField("testFloat", Schema.create(Schema.Type.FLOAT)),
                createPrimitiveUnionFieldSchema("testFloatUnion", Schema.Type.FLOAT),
                createField("testBoolean", Schema.create(Schema.Type.BOOLEAN)),
                createPrimitiveUnionFieldSchema("testBooleanUnion", Schema.Type.BOOLEAN),
                createField("testBytes", Schema.create(Schema.Type.BYTES)),
                createPrimitiveUnionFieldSchema("testBytesUnion", Schema.Type.BYTES));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testInt", 1);
        builder.set("testIntUnion", 1);
        builder.set("testString", "aaa");
        builder.set("testStringUnion", "aaa");
        builder.set("testLong", 1l);
        builder.set("testLongUnion", 1l);
        builder.set("testDouble", 1.0);
        builder.set("testDoubleUnion", 1.0);
        builder.set("testFloat", 1.0f);
        builder.set("testFloatUnion", 1.0f);
        builder.set("testBoolean", true);
        builder.set("testBooleanUnion", true);
        builder.set("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        builder.set("testBytesUnion", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals(1, record.get("testInt"));
        Assert.assertEquals(1, record.get("testIntUnion"));
        Assert.assertEquals("aaa", record.get("testString").toString());
        Assert.assertEquals("aaa", record.get("testStringUnion").toString());
        Assert.assertEquals(1l, record.get("testLong"));
        Assert.assertEquals(1l, record.get("testLongUnion"));
        Assert.assertEquals(1.0, record.get("testDouble"));
        Assert.assertEquals(1.0, record.get("testDoubleUnion"));
        Assert.assertEquals(1.0f, record.get("testFloat"));
        Assert.assertEquals(1.0f, record.get("testFloatUnion"));
        Assert.assertEquals(true, record.get("testBoolean"));
        Assert.assertEquals(true, record.get("testBooleanUnion"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0x01, 0x02 }), record.get("testBytes"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0x01, 0x02 }), record.get("testBytesUnion"));

    }

    @Test
    public void shouldWriteFixed() {
        // given
        Schema fixedSchema = createFixedSchema("testFixed", 2);
        Schema recordSchema = createRecord("testRecord", createField("testFixed", fixedSchema),
                createUnionField("testFixedUnion", fixedSchema), createArrayFieldSchema("testFixedArray", fixedSchema),
                createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testFixed", new GenericData.Fixed(fixedSchema, new byte[] { 0x01, 0x02 }));
        builder.set("testFixedUnion", new GenericData.Fixed(fixedSchema, new byte[] { 0x03, 0x04 }));
        builder.set("testFixedArray", Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[] { 0x05, 0x06 })));
        builder.set("testFixedUnionArray",
                Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[] { 0x07, 0x08 })));

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertArrayEquals(new byte[] { 0x01, 0x02 }, ((GenericData.Fixed) record.get("testFixed")).bytes());
        Assert.assertArrayEquals(new byte[] { 0x03, 0x04 }, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
        Assert.assertArrayEquals(new byte[] { 0x05, 0x06 },
                ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
        Assert.assertArrayEquals(new byte[] { 0x07, 0x08 },
                ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
    }

    @Test
    public void shouldWriteEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[] { "A", "B" });
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
                createUnionField("testEnumUnion", enumSchema), createArrayFieldSchema("testEnumArray", enumSchema),
                createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));
        builder.set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals("A", record.get("testEnum").toString());
        Assert.assertEquals("A", record.get("testEnumUnion").toString());
        Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
        Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
    }

    @Test
    public void shouldWriteSubRecordField() {
        // given
        Schema subRecordSchema = createRecord("subRecord",
                createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

        Schema recordSchema = createRecord("test", createUnionField("record", subRecordSchema),
                createField("record1", subRecordSchema), createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecordSchema);
        subRecordBuilder.set("subField", "abc");

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("record", subRecordBuilder.build());
        builder.set("record1", subRecordBuilder.build());
        builder.set("field", "abc");

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericRecord) record.get("record")).get("subField").toString());
        Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record")).getSchema().hashCode());
        Assert.assertEquals("abc", ((GenericRecord) record.get("record1")).get("subField").toString());
        Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record1")).getSchema().hashCode());
        Assert.assertEquals("abc", record.get("field").toString());
    }

    @Test
    public void shouldWriteSubRecordCollectionsField() {
        // given
        Schema subRecordSchema = createRecord("subRecord",
                createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
        Schema recordSchema = createRecord("test", createArrayFieldSchema("recordsArray", subRecordSchema),
                createMapFieldSchema("recordsMap", subRecordSchema),
                createUnionField("recordsArrayUnion", Schema.createArray(createUnionSchema(subRecordSchema))),
                createUnionField("recordsMapUnion", Schema.createMap(createUnionSchema(subRecordSchema))));

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecordSchema);
        subRecordBuilder.set("subField", "abc");

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        List<GenericData.Record> recordsArray = new ArrayList<>();
        recordsArray.add(subRecordBuilder.build());
        builder.set("recordsArray", recordsArray);
        builder.set("recordsArrayUnion", recordsArray);
        Map<String, GenericData.Record> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecordBuilder.build());
        builder.set("recordsMap", recordsMap);
        builder.set("recordsMapUnion", recordsMap);

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals("abc",
                ((List<GenericData.Record>) record.get("recordsArray")).get(0).get("subField").toString());
        Assert.assertEquals("abc",
                ((List<GenericData.Record>) record.get("recordsArrayUnion")).get(0).get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<String, GenericData.Record>) record.get("recordsMap")).get(new Utf8("1")).get("subField")
                        .toString());
        Assert.assertEquals("abc",
                ((Map<String, GenericData.Record>) record.get("recordsMapUnion")).get(new Utf8("1")).get("subField")
                        .toString());
    }

    @Test
    public void shouldWriteSubRecordComplexCollectionsField() {
        // given
        Schema subRecordSchema = createRecord("subRecord",
                createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));
        Schema recordSchema = createRecord(
                "test",
                createArrayFieldSchema("recordsArrayMap", Schema.createMap(createUnionSchema(subRecordSchema))),
                createMapFieldSchema("recordsMapArray", Schema.createArray(createUnionSchema(subRecordSchema))),
                createUnionField("recordsArrayMapUnion",
                        Schema.createArray(Schema.createMap(createUnionSchema(subRecordSchema)))),
                createUnionField("recordsMapArrayUnion",
                        Schema.createMap(Schema.createArray(createUnionSchema(subRecordSchema)))));

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecordSchema);
        subRecordBuilder.set("subField", "abc");

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        List<Map<String, GenericRecord>> recordsArrayMap = new ArrayList<>();
        Map<String, GenericRecord> recordMap = new HashMap<>();
        recordMap.put("1", subRecordBuilder.build());
        recordsArrayMap.add(recordMap);

        builder.set("recordsArrayMap", recordsArrayMap);
        builder.set("recordsArrayMapUnion", recordsArrayMap);

        Map<String, List<GenericRecord>> recordsMapArray = new HashMap<>();
        List<GenericRecord> recordList = new ArrayList<>();
        recordList.add(subRecordBuilder.build());
        recordsMapArray.put("1", recordList);

        builder.set("recordsMapArray", recordsMapArray);
        builder.set("recordsMapArrayUnion", recordsMapArray);

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals("abc",
                ((List<Map<Utf8, GenericRecord>>) record.get("recordsArrayMap")).get(0).get(new Utf8("1"))
                        .get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<Utf8, List<GenericRecord>>) record.get("recordsMapArray")).get(new Utf8("1")).get(0)
                        .get("subField").toString());
        Assert.assertEquals("abc",
                ((List<Map<Utf8, GenericRecord>>) record.get("recordsArrayMapUnion")).get(0).get(new Utf8("1"))
                        .get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<Utf8, List<GenericRecord>>) record.get("recordsMapArrayUnion")).get(new Utf8("1")).get(0)
                        .get("subField").toString());
    }

    @Test
    public void shouldWriteMultipleChoiceUnion() {
        // given
        Schema subRecordSchema = createRecord("subRecord",
                createPrimitiveUnionFieldSchema("subField", Schema.Type.STRING));

        Schema recordSchema = createRecord(
                "test",
                createUnionField("union", subRecordSchema, Schema.create(Schema.Type.STRING),
                        Schema.create(Schema.Type.INT)));

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecordSchema);
        subRecordBuilder.set("subField", "abc");

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", subRecordBuilder.build());

        // when
        GenericRecord record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericData.Record) record.get("union")).get("subField").toString());

        // given
        builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", "abc");

        // when
        record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("union").toString());

        // given
        builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", 1);

        // when
        record = deserializeGeneric(recordSchema, serializeGenericFast(builder.build()));

        // then
        Assert.assertEquals(1, record.get("union"));

    }

    @Test
    public void shouldWriteArrayOfRecords() {
        // given
        Schema recordSchema = createRecord("record",
                createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

        Schema arrayRecordSchema = Schema.createArray(recordSchema);

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        GenericData.Array<GenericData.Record> recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
        recordsArray.add(subRecordBuilder.build());
        recordsArray.add(subRecordBuilder.build());

        // when
        GenericData.Array<GenericRecord> array = deserializeGeneric(arrayRecordSchema,
                serializeGenericFast(recordsArray));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("field").toString());
        Assert.assertEquals("abc", array.get(1).get("field").toString());

        // given

        arrayRecordSchema = Schema.createArray(createUnionSchema(recordSchema));

        subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
        recordsArray.add(subRecordBuilder.build());
        recordsArray.add(subRecordBuilder.build());

        // when
        array = deserializeGeneric(arrayRecordSchema, serializeGenericFast(recordsArray));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("field").toString());
        Assert.assertEquals("abc", array.get(1).get("field").toString());
    }

    @Test
    public void shouldWriteMapOfRecords() {
        // given
        Schema recordSchema = createRecord("record",
                createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

        Schema mapRecordSchema = Schema.createMap(recordSchema);

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        Map<String, GenericData.Record> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecordBuilder.build());
        recordsMap.put("2", subRecordBuilder.build());

        // when
        Map<Utf8, GenericRecord> map = deserializeGeneric(mapRecordSchema,
                serializeGenericFast(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get(new Utf8("1")).get("field").toString());
        Assert.assertEquals("abc", map.get(new Utf8("2")).get("field").toString());

        // given
        mapRecordSchema = Schema.createMap(createUnionSchema(recordSchema));

        subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        recordsMap = new HashMap<>();
        recordsMap.put("1", subRecordBuilder.build());
        recordsMap.put("2", subRecordBuilder.build());

        // when
        map = deserializeGeneric(mapRecordSchema, serializeGenericFast(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get(new Utf8("1")).get("field").toString());
        Assert.assertEquals("abc", map.get(new Utf8("2")).get("field").toString());
    }

    @Test
    public void shouldSerializeNullElementInMap() {
        // given
        Schema mapRecordSchema = Schema.createMap(Schema.createUnion(
                Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));

        Map<String, Object> records = new HashMap<>();
        records.put("0", "0");
        records.put("1", null);
        records.put("2", 2);

        // when
        Map<Utf8, Object> map = deserializeGeneric(mapRecordSchema, serializeGenericFast(records, mapRecordSchema));

        // then
        Assert.assertEquals(3, map.size());
        Assert.assertEquals(new Utf8("0"), map.get(new Utf8("0")));
        Assert.assertNull(map.get(new Utf8("1")));
        Assert.assertEquals(2, map.get(new Utf8("2")));
    }

    @Test
    public void shouldSerializeNullElementInArray() {
        // given
        Schema arrayRecordSchema = Schema.createArray(Schema.createUnion(
                Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));

        List<Object> records = new ArrayList<>();
        records.add("0");
        records.add(null);
        records.add(2);

        // when
        List<Object> array = deserializeGeneric(arrayRecordSchema, serializeGenericFast(records, arrayRecordSchema));

        // then
        Assert.assertEquals(3, array.size());
        Assert.assertEquals(new Utf8("0"), array.get(0));
        Assert.assertNull(array.get(1));
        Assert.assertEquals(2, array.get(2));
    }

    private <T extends GenericContainer> Decoder serializeGenericFast(T data) {
        return serializeGenericFast(data, data.getSchema());
    }

    private <T> Decoder serializeGenericFast(T data, Schema schema) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);

        try {
            FastGenericSerializerGenerator<T> fastGenericSerializerGenerator = new FastGenericSerializerGenerator<>(
                    schema, tempDir, classLoader, null);
            FastSerializer<T> fastSerializer = fastGenericSerializerGenerator.generateSerializer();
            fastSerializer.serialize(data, binaryEncoder);
            binaryEncoder.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
    }

}
