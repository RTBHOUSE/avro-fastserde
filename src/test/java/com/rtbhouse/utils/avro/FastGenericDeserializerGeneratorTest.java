package com.rtbhouse.utils.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.addAliases;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createArrayFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createEnumSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createField;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createFixedSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createMapFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createPrimitiveFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createPrimitiveUnionFieldSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createRecord;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionField;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.serializeGeneric;

public class FastGenericDeserializerGeneratorTest {

    private File tempDir;
    private ClassLoader classLoader;

    @Before
    public void prepare() throws Exception {
        Path tempPath = Files.createTempDirectory("generated");
        tempDir = tempPath.toFile();

        classLoader = URLClassLoader.newInstance(new URL[]{tempDir.toURI().toURL()},
                FastGenericDeserializerGeneratorTest.class.getClassLoader());
    }

    @Test
    public void shouldReadPrimitives() {
        // given
        Schema javaLangStringSchema = Schema.create(Schema.Type.STRING);
        GenericData.setStringType(javaLangStringSchema, GenericData.StringType.String);
        Schema recordSchema = createRecord("testRecord",
                createField("testInt", Schema.create(Schema.Type.INT)),
                createPrimitiveUnionFieldSchema("testIntUnion", Schema.Type.INT),
                createField("testString", Schema.create(Schema.Type.STRING)),
                createPrimitiveUnionFieldSchema("testStringUnion", Schema.Type.STRING),
                createField("testJavaString", javaLangStringSchema),
                createUnionField("testJavaStringUnion", javaLangStringSchema),
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
        builder.set("testJavaString", "aaa");
        builder.set("testJavaStringUnion", "aaa");
        builder.set("testLong", 1L);
        builder.set("testLongUnion", 1L);
        builder.set("testDouble", 1.0);
        builder.set("testDoubleUnion", 1.0);
        builder.set("testFloat", 1.0f);
        builder.set("testFloatUnion", 1.0f);
        builder.set("testBoolean", true);
        builder.set("testBooleanUnion", true);
        builder.set("testBytes", ByteBuffer.wrap(new byte[]{0x01, 0x02}));
        builder.set("testBytesUnion", ByteBuffer.wrap(new byte[]{0x01, 0x02}));

        // when
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals(1, record.get("testInt"));
        Assert.assertEquals(1, record.get("testIntUnion"));
        Assert.assertEquals("aaa", record.get("testString").toString());
        Assert.assertEquals("aaa", record.get("testStringUnion").toString());
        Assert.assertEquals("aaa", record.get("testJavaString"));
        Assert.assertEquals("aaa", record.get("testJavaStringUnion"));
        Assert.assertEquals(1L, record.get("testLong"));
        Assert.assertEquals(1L, record.get("testLongUnion"));
        Assert.assertEquals(1.0, record.get("testDouble"));
        Assert.assertEquals(1.0, record.get("testDoubleUnion"));
        Assert.assertEquals(1.0f, record.get("testFloat"));
        Assert.assertEquals(1.0f, record.get("testFloatUnion"));
        Assert.assertEquals(true, record.get("testBoolean"));
        Assert.assertEquals(true, record.get("testBooleanUnion"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.get("testBytes"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[]{0x01, 0x02}), record.get("testBytesUnion"));

    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReadFixed() {
        // given
        Schema fixedSchema = createFixedSchema("testFixed", 2);
        Schema recordSchema = createRecord("testRecord", createField("testFixed", fixedSchema),
                createUnionField("testFixedUnion", fixedSchema), createArrayFieldSchema("testFixedArray", fixedSchema),
                createArrayFieldSchema("testFixedUnionArray", createUnionSchema(fixedSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testFixed", new GenericData.Fixed(fixedSchema, new byte[]{0x01, 0x02}));
        builder.set("testFixedUnion", new GenericData.Fixed(fixedSchema, new byte[]{0x03, 0x04}));
        builder.set("testFixedArray", Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[]{0x05, 0x06})));
        builder.set("testFixedUnionArray",
                Arrays.asList(new GenericData.Fixed(fixedSchema, new byte[]{0x07, 0x08})));

        // when
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertArrayEquals(new byte[]{0x01, 0x02}, ((GenericData.Fixed) record.get("testFixed")).bytes());
        Assert.assertArrayEquals(new byte[]{0x03, 0x04}, ((GenericData.Fixed) record.get("testFixedUnion")).bytes());
        Assert.assertArrayEquals(new byte[]{0x05, 0x06},
                ((List<GenericData.Fixed>) record.get("testFixedArray")).get(0).bytes());
        Assert.assertArrayEquals(new byte[]{0x07, 0x08},
                ((List<GenericData.Fixed>) record.get("testFixedUnionArray")).get(0).bytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReadEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B"});
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
                createUnionField("testEnumUnion", enumSchema), createArrayFieldSchema("testEnumArray", enumSchema),
                createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));
        builder.set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "A")));

        // when
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("A", record.get("testEnum").toString());
        Assert.assertEquals("A", record.get("testEnumUnion").toString());
        Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
        Assert.assertEquals("A", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReadPermutatedEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C", "D", "E"});
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema),
                createUnionField("testEnumUnion", enumSchema), createArrayFieldSchema("testEnumArray", enumSchema),
                createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema)));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "A"));
        builder.set("testEnumUnion", new GenericData.EnumSymbol(enumSchema, "B"));
        builder.set("testEnumArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "C")));
        builder.set("testEnumUnionArray", Arrays.asList(new GenericData.EnumSymbol(enumSchema, "D")));

        Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"B", "A", "D", "E", "C"});
        Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1),
                createUnionField("testEnumUnion", enumSchema1), createArrayFieldSchema("testEnumArray", enumSchema1),
                createArrayFieldSchema("testEnumUnionArray", createUnionSchema(enumSchema1)));

        // when
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema1, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("A", record.get("testEnum").toString());
        Assert.assertEquals("B", record.get("testEnumUnion").toString());
        Assert.assertEquals("C", ((List<GenericData.EnumSymbol>) record.get("testEnumArray")).get(0).toString());
        Assert.assertEquals("D", ((List<GenericData.EnumSymbol>) record.get("testEnumUnionArray")).get(0).toString());
    }

    @Test(expected = FastDeserializerGeneratorException.class)
    public void shouldNotReadStrippedEnum() {
        // given
        Schema enumSchema = createEnumSchema("testEnum", new String[]{"A", "B", "C"});
        Schema recordSchema = createRecord("testRecord", createField("testEnum", enumSchema));

        GenericRecordBuilder builder = new GenericRecordBuilder(recordSchema);
        builder.set("testEnum", new GenericData.EnumSymbol(enumSchema, "C"));

        Schema enumSchema1 = createEnumSchema("testEnum", new String[]{"A", "B"});
        Schema recordSchema1 = createRecord("testRecord", createField("testEnum", enumSchema1));

        // when
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema1, serializeGeneric(builder.build()));
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
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericRecord) record.get("record")).get("subField").toString());
        Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record")).getSchema().hashCode());
        Assert.assertEquals("abc", ((GenericRecord) record.get("record1")).get("subField").toString());
        Assert.assertEquals(subRecordSchema.hashCode(), ((GenericRecord) record.get("record1")).getSchema().hashCode());
        Assert.assertEquals("abc", record.get("field").toString());
    }

    @Test
    @SuppressWarnings("unchecked")
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
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc",
                ((List<GenericData.Record>) record.get("recordsArray")).get(0).get("subField").toString());
        Assert.assertEquals("abc",
                ((List<GenericData.Record>) record.get("recordsArrayUnion")).get(0).get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<Utf8, GenericData.Record>) record.get("recordsMap")).get(new Utf8("1")).get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<Utf8, GenericData.Record>) record.get("recordsMapUnion")).get(new Utf8("1")).get("subField").toString());
    }

    @Test
    @SuppressWarnings("unchecked")
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
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc",
                ((List<Map<Utf8, GenericRecord>>) record.get("recordsArrayMap")).get(0).get(new Utf8("1")).get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<Utf8, List<GenericRecord>>) record.get("recordsMapArray")).get(new Utf8("1")).get(0).get("subField").toString());
        Assert.assertEquals("abc",
                ((List<Map<Utf8, GenericRecord>>) record.get("recordsArrayMapUnion")).get(0).get(new Utf8("1"))
                        .get("subField").toString());
        Assert.assertEquals("abc",
                ((Map<Utf8, List<GenericRecord>>) record.get("recordsMapArrayUnion")).get(new Utf8("1")).get(0)
                        .get("subField").toString());
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
        GenericRecord record = deserializeGenericFast(record1Schema, record2Schema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("testString").toString());
        Assert.assertEquals("def", record.get("testStringUnionAlias").toString());
    }

    @Test
    @SuppressWarnings("unchecked")
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
        GenericRecord record = deserializeGenericFast(record1Schema, record2Schema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("testNotRemoved").toString());
        Assert.assertNull(record.get("testRemoved"));
        Assert.assertEquals("ghi", record.get("testNotRemoved2").toString());
        Assert.assertEquals("ghi", ((GenericRecord) record.get("subRecord")).get("testNotRemoved2").toString());
        Assert.assertEquals("ghi", ((List<GenericRecord>) record.get("subRecordArray")).get(0).get("testNotRemoved2").toString());
        Assert.assertEquals("ghi",
                ((Map<Utf8, GenericRecord>) record.get("subRecordMap")).get(new Utf8("1")).get("testNotRemoved2").toString());
    }

    @Test
    public void shouldSkipRemovedRecord() {
        // given
        Schema subRecord1Schema = createRecord("subRecord",
                createPrimitiveFieldSchema("test1", Schema.Type.STRING),
                createPrimitiveFieldSchema("test2", Schema.Type.STRING));
        Schema subRecord2Schema = createRecord("subRecord2",
                createPrimitiveFieldSchema("test1", Schema.Type.STRING),
                createPrimitiveFieldSchema("test2", Schema.Type.STRING));

        Schema record1Schema = createRecord("test",
                createField("subRecord1", subRecord1Schema),
                createField("subRecord2", subRecord2Schema),
                createUnionField("subRecord3", subRecord2Schema),
                createField("subRecord4", subRecord1Schema));

        Schema record2Schema = createRecord("test",
                createField("subRecord1", subRecord1Schema),
                createField("subRecord4", subRecord1Schema));

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecord1Schema);
        subRecordBuilder.set("test1", "abc");
        subRecordBuilder.set("test2", "def");

        GenericRecordBuilder subRecordBuilder2 = new GenericRecordBuilder(subRecord2Schema);
        subRecordBuilder2.set("test1", "ghi");
        subRecordBuilder2.set("test2", "jkl");

        GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
        builder.set("subRecord1", subRecordBuilder.build());
        builder.set("subRecord2", subRecordBuilder2.build());
        builder.set("subRecord3", subRecordBuilder2.build());
        builder.set("subRecord4", subRecordBuilder.build());

        // when
        GenericRecord record = deserializeGenericFast(record1Schema, record2Schema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericRecord) record.get("subRecord1")).get("test1").toString());
        Assert.assertEquals("def", ((GenericRecord) record.get("subRecord1")).get("test2").toString());
        Assert.assertEquals("abc", ((GenericRecord) record.get("subRecord4")).get("test1").toString());
        Assert.assertEquals("def", ((GenericRecord) record.get("subRecord4")).get("test2").toString());
    }

    @Test
    public void shouldSkipRemovedNestedRecord() {
        // given
        Schema subSubRecordSchema = createRecord("subSubRecord",
                createPrimitiveFieldSchema("test1", Schema.Type.STRING),
                createPrimitiveFieldSchema("test2", Schema.Type.STRING));
        Schema subRecord1Schema = createRecord("subRecord",
                createPrimitiveFieldSchema("test1", Schema.Type.STRING),
                createField("test2", subSubRecordSchema),
                createUnionField("test3", subSubRecordSchema),
                createPrimitiveFieldSchema("test4", Schema.Type.STRING));
        Schema subRecord2Schema = createRecord("subRecord",
                createPrimitiveFieldSchema("test1", Schema.Type.STRING),
                createPrimitiveFieldSchema("test4", Schema.Type.STRING));

        Schema record1Schema = createRecord("test",
                createField("subRecord", subRecord1Schema));

        Schema record2Schema = createRecord("test",
                createField("subRecord", subRecord2Schema));

        GenericRecordBuilder subSubRecordBuilder = new GenericRecordBuilder(subSubRecordSchema);
        subSubRecordBuilder.set("test1", "abc");
        subSubRecordBuilder.set("test2", "def");

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(subRecord1Schema);
        subRecordBuilder.set("test1", "abc");
        subRecordBuilder.set("test2", subSubRecordBuilder.build());
        subRecordBuilder.set("test3", subSubRecordBuilder.build());
        subRecordBuilder.set("test4", "def");

        GenericRecordBuilder builder = new GenericRecordBuilder(record1Schema);
        builder.set("subRecord", subRecordBuilder.build());

        // when
        GenericRecord record = deserializeGenericFast(record1Schema, record2Schema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericRecord) record.get("subRecord")).get("test1").toString());
        Assert.assertEquals("def", ((GenericRecord) record.get("subRecord")).get("test4").toString());
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
        GenericRecord record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", ((GenericData.Record) record.get("union")).get("subField").toString());

        // given
        builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", "abc");

        // when
        record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

        // then
        Assert.assertEquals("abc", record.get("union").toString());

        // given
        builder = new GenericRecordBuilder(recordSchema);
        builder.set("union", 1);

        // when
        record = deserializeGenericFast(recordSchema, recordSchema, serializeGeneric(builder.build()));

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
        GenericData.Array<GenericRecord> array = deserializeGenericFast(arrayRecordSchema, arrayRecordSchema,
                serializeGeneric(recordsArray));

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
        array = deserializeGenericFast(arrayRecordSchema, arrayRecordSchema, serializeGeneric(recordsArray));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("field").toString());
        Assert.assertEquals("abc", array.get(1).get("field").toString());
    }

    @Test
    public void shouldReadArrayOfPrimitives() {
        // given
        Schema stringArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));

        GenericData.Array<String> stringArray = new GenericData.Array<>(0, stringArraySchema);
        stringArray.add("aaa");
        stringArray.add("abc");

        Schema intArraySchema = Schema.createArray(Schema.create(Schema.Type.INT));

        GenericData.Array<Integer> intArray = new GenericData.Array<>(0, intArraySchema);
        intArray.add(1);
        intArray.add(2);

        Schema longArraySchema = Schema.createArray(Schema.create(Schema.Type.LONG));

        GenericData.Array<Long> longArray = new GenericData.Array<>(0, longArraySchema);
        longArray.add(1L);
        longArray.add(2L);

        Schema doubleArraySchema = Schema.createArray(Schema.create(Schema.Type.DOUBLE));

        GenericData.Array<Double> doubleArray = new GenericData.Array<>(0, doubleArraySchema);
        doubleArray.add(1.0);
        doubleArray.add(2.0);

        Schema floatArraySchema = Schema.createArray(Schema.create(Schema.Type.FLOAT));

        GenericData.Array<Float> floatArray = new GenericData.Array<>(0, floatArraySchema);
        floatArray.add(1.0f);
        floatArray.add(2.0f);

        Schema bytesArraySchema = Schema.createArray(Schema.create(Schema.Type.BYTES));

        GenericData.Array<ByteBuffer> bytesArray = new GenericData.Array<>(0, bytesArraySchema);
        bytesArray.add(ByteBuffer.wrap(new byte[]{0x01}));
        bytesArray.add(ByteBuffer.wrap(new byte[]{0x02}));

        // when
        GenericData.Array<Utf8> resultStringArray = deserializeGenericFast(stringArraySchema, stringArraySchema,
                serializeGeneric(stringArray));

        GenericData.Array<Integer> resultIntegerArray = deserializeGenericFast(intArraySchema, intArraySchema,
                serializeGeneric(intArray));

        GenericData.Array<Long> resultLongArray = deserializeGenericFast(longArraySchema, longArraySchema,
                serializeGeneric(longArray));

        GenericData.Array<Double> resultDoubleArray = deserializeGenericFast(doubleArraySchema, doubleArraySchema,
                serializeGeneric(doubleArray));

        GenericData.Array<Float> resultFloatArray = deserializeGenericFast(floatArraySchema, floatArraySchema,
                serializeGeneric(floatArray));

        GenericData.Array<ByteBuffer> resultBytesArray = deserializeGenericFast(bytesArraySchema, bytesArraySchema,
                serializeGeneric(bytesArray));

        // then
        Assert.assertEquals(2, resultStringArray.size());
        Assert.assertEquals("aaa", resultStringArray.get(0).toString());
        Assert.assertEquals("abc", resultStringArray.get(1).toString());

        Assert.assertEquals(2, resultIntegerArray.size());
        Assert.assertEquals(Integer.valueOf(1), resultIntegerArray.get(0));
        Assert.assertEquals(Integer.valueOf(2), resultIntegerArray.get(1));

        Assert.assertEquals(2, resultLongArray.size());
        Assert.assertEquals(Long.valueOf(1L), resultLongArray.get(0));
        Assert.assertEquals(Long.valueOf(2L), resultLongArray.get(1));

        Assert.assertEquals(2, resultDoubleArray.size());
        Assert.assertEquals(Double.valueOf(1.0), resultDoubleArray.get(0));
        Assert.assertEquals(Double.valueOf(2.0), resultDoubleArray.get(1));

        Assert.assertEquals(2, resultFloatArray.size());
        Assert.assertEquals(Float.valueOf(1f), resultFloatArray.get(0));
        Assert.assertEquals(Float.valueOf(2f), resultFloatArray.get(1));

        Assert.assertEquals(2, resultBytesArray.size());
        Assert.assertEquals(0x01, resultBytesArray.get(0).get());
        Assert.assertEquals(0x02, resultBytesArray.get(1).get());
    }

    @Test
    public void shouldReadArrayOfJavaStrings() {
        // given
        Schema javaStringSchema = Schema.create(Schema.Type.STRING);
        GenericData.setStringType(javaStringSchema, GenericData.StringType.String);
        Schema javaStringArraySchema = Schema.createArray(javaStringSchema);

        GenericData.Array<String> javaStringArray = new GenericData.Array<>(0, javaStringArraySchema);
        javaStringArray.add("aaa");
        javaStringArray.add("abc");

        GenericData.Array<String> resultJavaStringArray = deserializeGenericFast(javaStringArraySchema, javaStringArraySchema,
                serializeGeneric(javaStringArray));

        // then
        Assert.assertEquals(2, resultJavaStringArray.size());
        Assert.assertEquals("aaa", resultJavaStringArray.get(0));
        Assert.assertEquals("abc", resultJavaStringArray.get(1));
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
        Map<Utf8, GenericRecord> map = deserializeGenericFast(mapRecordSchema, mapRecordSchema,
                serializeGeneric(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get(new Utf8("1")).get("field").toString());
        Assert.assertEquals("abc", map.get(new Utf8("2")).get("field").toString());

        // given
        mapRecordSchema = Schema.createMap(createUnionSchema(recordSchema));

        // when
        map = deserializeGenericFast(mapRecordSchema, mapRecordSchema, serializeGeneric(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get(new Utf8("1")).get("field").toString());
        Assert.assertEquals("abc", map.get(new Utf8("2")).get("field").toString());
    }

    @Test
    public void shouldReadMapOfPrimitives() {
        // given
        Schema stringMapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

        Map<String, String> stringMap = new HashMap<>(0);
        stringMap.put("1", "abc");
        stringMap.put("2", "aaa");

        Schema intMapSchema = Schema.createMap(Schema.create(Schema.Type.INT));

        Map<String, Integer> intMap = new HashMap<>(0);
        intMap.put("1", 1);
        intMap.put("2", 2);

        Schema longMapSchema = Schema.createMap(Schema.create(Schema.Type.LONG));

        Map<String, Long> longMap = new HashMap<>(0);
        longMap.put("1", 1L);
        longMap.put("2", 2L);

        Schema doubleMapSchema = Schema.createMap(Schema.create(Schema.Type.DOUBLE));

        Map<String, Double> doubleMap = new HashMap<>(0);
        doubleMap.put("1", 1.0);
        doubleMap.put("2", 2.0);

        Schema floatMapSchema = Schema.createMap(Schema.create(Schema.Type.FLOAT));

        Map<String, Float> floatMap = new HashMap<>(0);
        floatMap.put("1", 1.0f);
        floatMap.put("2", 2.0f);

        Schema bytesMapSchema = Schema.createMap(Schema.create(Schema.Type.BYTES));

        Map<String, ByteBuffer> bytesMap = new HashMap<>(0);
        bytesMap.put("1", ByteBuffer.wrap(new byte[]{0x01}));
        bytesMap.put("2", ByteBuffer.wrap(new byte[]{0x02}));

        // when
        Map<Utf8, Utf8> resultStringMap = deserializeGenericFast(stringMapSchema, stringMapSchema,
                serializeGeneric(stringMap, stringMapSchema));

        Map<Utf8, Integer> resultIntegerMap = deserializeGenericFast(intMapSchema, intMapSchema,
                serializeGeneric(intMap, intMapSchema));

        Map<Utf8, Long> resultLongMap = deserializeGenericFast(longMapSchema, longMapSchema,
                serializeGeneric(longMap, longMapSchema));

        Map<Utf8, Double> resultDoubleMap = deserializeGenericFast(doubleMapSchema, doubleMapSchema,
                serializeGeneric(doubleMap, doubleMapSchema));

        Map<Utf8, Float> resultFloatMap = deserializeGenericFast(floatMapSchema, floatMapSchema,
                serializeGeneric(floatMap, floatMapSchema));

        Map<Utf8, ByteBuffer> resultBytesMap = deserializeGenericFast(bytesMapSchema, bytesMapSchema,
                serializeGeneric(bytesMap, bytesMapSchema));

        // then
        Assert.assertEquals(2, resultStringMap.size());
        Assert.assertEquals("abc", resultStringMap.get(new Utf8("1")).toString());
        Assert.assertEquals("aaa", resultStringMap.get(new Utf8("2")).toString());

        Assert.assertEquals(2, resultIntegerMap.size());
        Assert.assertEquals(Integer.valueOf(1), resultIntegerMap.get(new Utf8("1")));
        Assert.assertEquals(Integer.valueOf(2), resultIntegerMap.get(new Utf8("2")));

        Assert.assertEquals(2, resultLongMap.size());
        Assert.assertEquals(Long.valueOf(1L), resultLongMap.get(new Utf8("1")));
        Assert.assertEquals(Long.valueOf(2L), resultLongMap.get(new Utf8("2")));

        Assert.assertEquals(2, resultDoubleMap.size());
        Assert.assertEquals(Double.valueOf(1.0), resultDoubleMap.get(new Utf8("1")));
        Assert.assertEquals(Double.valueOf(2.0), resultDoubleMap.get(new Utf8("2")));

        Assert.assertEquals(2, resultFloatMap.size());
        Assert.assertEquals(Float.valueOf(1f), resultFloatMap.get(new Utf8("1")));
        Assert.assertEquals(Float.valueOf(2f), resultFloatMap.get(new Utf8("2")));

        Assert.assertEquals(2, resultBytesMap.size());
        Assert.assertEquals(0x01, resultBytesMap.get(new Utf8("1")).get());
        Assert.assertEquals(0x02, resultBytesMap.get(new Utf8("2")).get());
    }

    @Test
    public void shouldReadMapOfJavaStrings() {
        // given
        Schema stringMapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
        Schema javaStringSchema = Schema.create(Schema.Type.STRING);
        GenericData.setStringType(javaStringSchema, GenericData.StringType.String);
        Schema javaStringMapSchema = Schema.createMap(javaStringSchema);

        Map<String, String> stringMap = new HashMap<>(0);
        stringMap.put("1", "abc");
        stringMap.put("2", "aaa");

        // when
        Map<Utf8, String> resultJavaStringMap = deserializeGenericFast(stringMapSchema, javaStringMapSchema,
                serializeGeneric(stringMap, javaStringMapSchema));

        // then
        Assert.assertEquals(2, resultJavaStringMap.size());
        Assert.assertEquals("abc", resultJavaStringMap.get(new Utf8("1")));
        Assert.assertEquals("aaa", resultJavaStringMap.get(new Utf8("2")));
    }

    @Test
    public void shouldReadJavaStringKeyedMapOfRecords() {
        // given
        Schema recordSchema = createRecord("record",
                createPrimitiveUnionFieldSchema("field", Schema.Type.STRING));

        Schema mapRecordSchema = Schema.createMap(recordSchema);
        GenericData.setStringType(mapRecordSchema, GenericData.StringType.String);

        GenericRecordBuilder subRecordBuilder = new GenericRecordBuilder(recordSchema);
        subRecordBuilder.set("field", "abc");

        Map<String, GenericData.Record> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecordBuilder.build());
        recordsMap.put("2", subRecordBuilder.build());

        // when
        Map<String, GenericRecord> mapWithStringKeys = deserializeGenericFast(mapRecordSchema, mapRecordSchema, serializeGeneric(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, mapWithStringKeys.size());
        Assert.assertEquals("abc", mapWithStringKeys.get("1").get("field").toString());
        Assert.assertEquals("abc", mapWithStringKeys.get("2").get("field").toString());
    }

    @Test
    public void shouldDeserializeNullElementInMap() {
        // given
        Schema mapRecordSchema = Schema.createMap(Schema.createUnion(
                Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));

        Map<String, Object> records = new HashMap<>();
        records.put("0", "0");
        records.put("1", null);
        records.put("2", 2);

        // when
        Map<Utf8, Object> map = deserializeGenericFast(mapRecordSchema, mapRecordSchema,
                serializeGeneric(records, mapRecordSchema));

        // then
        Assert.assertEquals(3, map.size());
        Assert.assertEquals("0", map.get(new Utf8("0")).toString());
        Assert.assertNull(map.get(new Utf8("1")));
        Assert.assertEquals(2, map.get(new Utf8("2")));
    }

    @Test
    public void shouldDeserializeNullElementInArray() {
        // given
        Schema arrayRecordSchema = Schema.createArray(Schema.createUnion(
                Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));

        List<Object> records = new ArrayList<>();
        records.add("0");
        records.add(null);
        records.add(2);

        // when
        List<Object> array = deserializeGenericFast(arrayRecordSchema, arrayRecordSchema,
                serializeGeneric(records, arrayRecordSchema));

        // then
        Assert.assertEquals(3, array.size());
        Assert.assertEquals("0", array.get(0).toString());
        Assert.assertNull(array.get(1));
        Assert.assertEquals(2, array.get(2));
    }

    private <T> T deserializeGenericFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
        FastDeserializer<T> deserializer = new FastGenericDeserializerGenerator<T>(writerSchema,
                readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
