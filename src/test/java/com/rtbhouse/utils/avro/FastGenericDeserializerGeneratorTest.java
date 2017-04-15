package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.addAliases;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createArrayFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createEnumSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createField;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createFixedSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createMapFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createPrimitiveUnionFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createRecord;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionField;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.genericDataAsDecoder;

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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FastGenericDeserializerGeneratorTest {

    private File tempDir;
    private ClassLoader classLoader;

    @Before
    public void prepare() throws Exception {
        Path tempPath = Files.createTempDirectory("generated");
        tempDir = tempPath.toFile();

        classLoader = URLClassLoader.newInstance(new URL[]{ tempDir.toURI().toURL() },
            FastGenericDeserializerGeneratorTest.class.getClassLoader());
    }

    @Test
    public void shouldReadPrimitives() {
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
        builder.set("testBytes", ByteBuffer.wrap(new byte[]{ 0x01, 0x02 }));
        builder.set("testBytesUnion", ByteBuffer.wrap(new byte[]{ 0x01, 0x02 }));

        // when
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals(1, record.get("testInt"));
        Assert.assertEquals(1, record.get("testIntUnion"));
        Assert.assertEquals("aaa", record.get("testString"));
        Assert.assertEquals("aaa", record.get("testStringUnion"));
        Assert.assertEquals(1l, record.get("testLong"));
        Assert.assertEquals(1l, record.get("testLongUnion"));
        Assert.assertEquals(1.0, record.get("testDouble"));
        Assert.assertEquals(1.0, record.get("testDoubleUnion"));
        Assert.assertEquals(1.0f, record.get("testFloat"));
        Assert.assertEquals(1.0f, record.get("testFloatUnion"));
        Assert.assertEquals(true, record.get("testBoolean"));
        Assert.assertEquals(true, record.get("testBooleanUnion"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[]{ 0x01, 0x02 }), record.get("testBytes"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[]{ 0x01, 0x02 }), record.get("testBytesUnion"));

    }

    @Test
    public void shouldReadFixed() {
        // given
        Schema fixedSchema = createFixedSchema("testFixed", 2);
        Schema recordSchema = createRecord("testRecord", createField("testFixed", fixedSchema),
            createUnionField("testFixedUnion", fixedSchema), createArrayFieldSchema("testFixedArray", fixedSchema),
            createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testFixed", new GenericData.Fixed(fixedSchema, new byte[]{ 0x01, 0x02 }));
        builder.set("testFixedUnion", new GenericData.Fixed(fixedSchema, new byte[]{ 0x03, 0x04 }));
        builder.set("testFixedArray", Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[]{ 0x05, 0x06 })));
        builder.set("testFixedUnionArray", Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[]{ 0x07, 0x08 })));

        // when
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertArrayEquals(new byte[]{ 0x01, 0x02 }, ((GenericData.Fixed) record.get("testFixed")).bytes());
        Assert.assertArrayEquals(new byte[]{ 0x03, 0x04 }, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
        Assert.assertArrayEquals(new byte[]{ 0x05, 0x06 }, ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
        Assert.assertArrayEquals(new byte[]{ 0x07, 0x08 }, ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
    }

    @Test
    public void shouldReadEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[]{ "A", "B" });
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
            createUnionField("testEnumUnion", enumSchema), createArrayFieldSchema("testEnumArray", enumSchema),
            createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));
        builder.set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));

        // when
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("A", record.get("testEnum").toString());
        Assert.assertEquals("A", record.get("testEnumUnion").toString());
        Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
        Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
    }

    @Test
    public void shouldReadPermutatedEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[]{ "A", "B", "C", "D", "E" });
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
            createUnionField("testEnumUnion", enumSchema), createArrayFieldSchema("testEnumArray", enumSchema),
            createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "B"));
        builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "C")));
        builder.set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "D")));

        Schema enumSchema1 = createEnumSchema("testEnum", new String[]{ "B", "A", "D", "E", "C" });
        Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1),
            createUnionField("testEnumUnion", enumSchema1), createArrayFieldSchema("testEnumArray", enumSchema1),
            createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema1)));

        // when
        GenericRecord record = decodeRecord(recordSchema, recordSchema1, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("A", record.get("testEnum").toString());
        Assert.assertEquals("B", record.get("testEnumUnion").toString());
        Assert.assertEquals("C", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
        Assert.assertEquals("D", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
    }

    @Test(expected = FastDeserializerGeneratorException.class)
    public void shouldNotReadStrippedEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[]{ "A", "B", "C" });
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "C"));

        Schema enumSchema1 = createEnumSchema("testEnum", new String[]{ "A", "B" });
        Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1));

        // when
        GenericRecord record = decodeRecord(recordSchema, recordSchema1, genericDataAsDecoder(builder.build()));
    }

    @Test
    public void shouldReadSubRecordField() {
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
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericRecord) record.get("record")).get("subField"));
        Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record")).getSchema().hashCode());
        Assert.assertEquals("abc", ((GenericRecord) record.get("record1")).get("subField"));
        Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record1")).getSchema().hashCode());
        Assert.assertEquals("abc", record.get("field"));
    }

    @Test
    public void shouldReadSubRecordCollectionsField() {
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
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc",
            ((List<GenericData.Record>) record.get("recordsArray")).get(0).get("subField"));
        Assert.assertEquals("abc",
            ((List<GenericData.Record>) record.get("recordsArrayUnion")).get(0).get("subField"));
        Assert.assertEquals("abc",
            ((Map<String, GenericData.Record>) record.get("recordsMap")).get("1").get("subField"));
        Assert.assertEquals("abc",
            ((Map<String, GenericData.Record>) record.get("recordsMapUnion")).get("1").get("subField"));
    }

    @Test
    public void shouldReadSubRecordComplexCollectionsField() {
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
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc",
            ((List<Map<String, GenericRecord>>) record.get("recordsArrayMap")).get(0).get("1").get("subField"));
        Assert.assertEquals("abc",
            ((Map<String, List<GenericRecord>>) record.get("recordsMapArray")).get("1").get(0).get("subField"));
        Assert.assertEquals("abc",
            ((List<Map<String, GenericRecord>>) record.get("recordsArrayMapUnion")).get(0).get("1").get("subField"));
        Assert.assertEquals("abc",
            ((Map<String, List<GenericRecord>>) record.get("recordsMapArrayUnion")).get("1").get(0).get("subField"));
    }

    @Test
    public void shouldReadAliasedField() {
        // given
        Schema record1Schema = createRecord("test", createPrimitiveUnionFieldSchema("testString", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING));
        Schema record2Schema = createRecord(
            "test",
            createPrimitiveUnionFieldSchema("testString", Schema.Type.STRING),
            addAliases(createPrimitiveUnionFieldSchema("testStringUnionAlias", Schema.Type.STRING),
                "testStringUnion"));

        GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
        builder.set("testString", "abc");
        builder.set("testStringUnion", "def");

        // when
        GenericRecord record = decodeRecord(record1Schema, record2Schema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("testString"));
        Assert.assertEquals("def", record.get("testStringUnionAlias"));
    }

    @Test
    public void shouldSkipRemovedField() {
        // given
        Schema subRecord1Schema = createRecord("subRecord",
            createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
        Schema record1Schema = createRecord("test",
            createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
            createUnionField("subRecord", subRecord1Schema),
            createMapFieldSchema("subRecordMap", subRecord1Schema),
            createArrayFieldSchema("subRecordArray", subRecord1Schema));
        Schema subRecord2Schema = createRecord("subRecord",
            createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING));
        Schema record2Schema = createRecord("test",
            createPrimitiveUnionFieldSchema("testNotRemoved", Schema.Type.STRING),
            createPrimitiveUnionFieldSchema("testNotRemoved2", Schema.Type.STRING),
            createUnionField("subRecord", subRecord2Schema),
            createMapFieldSchema("subRecordMap", subRecord2Schema),
            createArrayFieldSchema("subRecordArray", subRecord2Schema));

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecord1Schema);
        subRecordBuilder.set("testNotRemoved", "abc");
        subRecordBuilder.set("testRemoved", "def");
        subRecordBuilder.set("testNotRemoved2", "ghi");

        GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
        builder.set("testNotRemoved", "abc");
        builder.set("testRemoved", "def");
        builder.set("testNotRemoved2", "ghi");
        builder.set("subRecord", subRecordBuilder.build());
        builder.set("subRecordArray", Arrays.asList(subRecordBuilder.build()));

        Map<String, GenericRecord> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecordBuilder.build());
        builder.set("subRecordMap", recordsMap);

        // when
        GenericRecord record = decodeRecord(record1Schema, record2Schema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("testNotRemoved"));
        Assert.assertNull(record.get("testRemoved"));
        Assert.assertEquals("ghi", record.get("testNotRemoved2"));
        Assert.assertEquals("ghi", ((GenericRecord) record.get("subRecord")).get("testNotRemoved2"));
        Assert.assertEquals("ghi", ((List<GenericRecord>) record.get("subRecordArray")).get(0).get("testNotRemoved2"));
        Assert.assertEquals("ghi",
            ((Map<String, GenericRecord>) record.get("subRecordMap")).get("1").get("testNotRemoved2"));
    }

    @Test
    public void shouldReadMultipleChoiceUnion() {
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
        GenericRecord record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericData.Record) record.get("union")).get("subField"));

        // given
        builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", "abc");

        // when
        record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("union"));

        // given
        builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", 1);

        // when
        record = decodeRecord(recordSchema, recordSchema, genericDataAsDecoder(builder.build()));

        // then
        Assert.assertEquals(1, record.get("union"));

    }

    @Test
    public void shouldReadArrayOfRecords() {
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
        GenericData.Array<GenericRecord> array = decodeRecord(arrayRecordSchema, arrayRecordSchema,
            genericDataAsDecoder(recordsArray));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("field"));
        Assert.assertEquals("abc", array.get(1).get("field"));

        // given

        arrayRecordSchema = Schema.createArray(createUnionSchema(recordSchema));

        subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        recordsArray = new GenericData.Array<>(0, arrayRecordSchema);
        recordsArray.add(subRecordBuilder.build());
        recordsArray.add(subRecordBuilder.build());

        // when
        array = decodeRecord(arrayRecordSchema, arrayRecordSchema, genericDataAsDecoder(recordsArray));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("field"));
        Assert.assertEquals("abc", array.get(1).get("field"));
    }

    @Test
    public void shouldReadMapOfRecords() {
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
        Map<String, GenericRecord> map = decodeRecord(mapRecordSchema, mapRecordSchema,
            FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get("1").get("field"));
        Assert.assertEquals("abc", map.get("2").get("field"));

        // given
        mapRecordSchema = Schema.createMap(createUnionSchema(recordSchema));

        subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        recordsMap = new HashMap<>();
        recordsMap.put("1", subRecordBuilder.build());
        recordsMap.put("2", subRecordBuilder.build());

        // when
        map = decodeRecord(mapRecordSchema, mapRecordSchema, FastSerdeTestsSupport.genericDataAsDecoder(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get("1").get("field"));
        Assert.assertEquals("abc", map.get("2").get("field"));
    }

    public <T> T decodeRecord(Schema writerSchema, Schema readerSchema, Decoder decoder) {
        FastDeserializer<T> deserializer = new FastGenericDeserializerGenerator<T>(writerSchema,
            readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
