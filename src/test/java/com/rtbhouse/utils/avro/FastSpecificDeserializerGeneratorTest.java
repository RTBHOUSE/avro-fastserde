package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.createUnionSchema;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.genericDataAsDecoder;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.specificDataAsDecoder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Decoder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rtbhouse.utils.generated.avro.SubRecord;
import com.rtbhouse.utils.generated.avro.TestEnum;
import com.rtbhouse.utils.generated.avro.TestFixed;
import com.rtbhouse.utils.generated.avro.TestRecord;

public class FastSpecificDeserializerGeneratorTest {

    private File tempDir;
    private ClassLoader classLoader;

    @Before
    public void prepare() throws Exception {
        Path tempPath = Files.createTempDirectory("generated");
        tempDir = tempPath.toFile();

        classLoader = URLClassLoader.newInstance(new URL[] { tempDir.toURI().toURL() },
                FastSpecificDeserializerGeneratorTest.class.getClassLoader());
    }

    @Test
    public void shouldReadPrimitives() {
        TestRecord record = emptyTestRecord();
        record.put("testInt", 1);
        record.put("testIntUnion", 1);
        record.put("testString", "aaa");
        record.put("testStringUnion", "aaa");
        record.put("testLong", 1l);
        record.put("testLongUnion", 1l);
        record.put("testDouble", 1.0);
        record.put("testDoubleUnion", 1.0);
        record.put("testFloat", 1.0f);
        record.put("testFloatUnion", 1.0f);
        record.put("testBoolean", true);
        record.put("testBooleanUnion", true);
        record.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        record.put("testBytesUnion", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

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
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0x01, 0x02 }), record.get("testBytes"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0x01, 0x02 }), record.get("testBytesUnion"));

    }

    @Test
    public void shouldReadFixed() {
        // given
        TestRecord record = emptyTestRecord();

        record.put("testFixed", new TestFixed(new byte[] { 0x01 }));
        record.put("testFixedUnion", new TestFixed(new byte[] { 0x02 }));
        record.put("testFixedArray", Arrays.asList(new TestFixed(new byte[] { 0x03 })));
        record.put("testFixedUnionArray", Arrays.asList(new TestFixed(new byte[] { 0x04 })));

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertArrayEquals(new byte[] { 0x01 }, record.getTestFixed().bytes());
        Assert.assertArrayEquals(new byte[] { 0x02 }, record.getTestFixedUnion().bytes());
        Assert.assertArrayEquals(new byte[] { 0x03 }, record.getTestFixedArray().get(0).bytes());
        Assert.assertArrayEquals(new byte[] { 0x04 }, record.getTestFixedUnionArray().get(0).bytes());
    }

    @Test
    public void shouldReadEnum() {
        // given
        TestRecord record = emptyTestRecord();

        record.put("testEnum", TestEnum.A);
        record.put("testEnumUnion", TestEnum.A);
        record.put("testEnumArray", Arrays.asList(TestEnum.A));
        record.put("testEnumUnionArray", Arrays.asList(TestEnum.A));

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals(TestEnum.A, record.getTestEnum());
        Assert.assertEquals(TestEnum.A, record.getTestEnumUnion());
        Assert.assertEquals(TestEnum.A, record.getTestEnumArray().get(0));
        Assert.assertEquals(TestEnum.A, record.getTestEnumUnionArray().get(0));
    }

    @Test
    public void shouldReadPermutedEnum() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));
        GenericData.Fixed testFixed = new GenericData.Fixed(oldRecordSchema.getField("testFixed").schema(),
                new byte[] { 0x01 });
        GenericData.Record subRecord = new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema()
                .getTypes().get(1));
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        oldRecord.put("testInt", 1);
        oldRecord.put("testLong", 1l);
        oldRecord.put("testDouble", 1.0);
        oldRecord.put("testFloat", 1.0f);
        oldRecord.put("testBoolean", true);
        oldRecord.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        oldRecord.put("testString", "aaa");
        oldRecord.put("testFixed", testFixed);
        oldRecord.put("testFixedUnion", testFixed);
        oldRecord.put("testFixedArray", Arrays.asList(testFixed));
        oldRecord.put("testFixedUnionArray", Arrays.asList(testFixed));
        oldRecord.put("testEnum", new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(), "A"));
        oldRecord.put("testEnumUnion", new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(), "B"));
        oldRecord.put("testEnumArray",
                Arrays.asList(new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(), "C")));
        oldRecord.put("testEnumUnionArray",
                Arrays.asList(new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(), "D")));

        oldRecord.put("subRecordUnion", subRecord);
        oldRecord.put("subRecord", subRecord);

        oldRecord.put("recordsArray", Collections.emptyList());
        oldRecord.put("recordsArrayMap", Collections.emptyList());
        oldRecord.put("recordsMap", Collections.emptyMap());
        oldRecord.put("recordsMapArray", Collections.emptyMap());

        // when
        TestRecord record = decodeRecord(TestRecord.getClassSchema(), oldRecordSchema,
                genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals(TestEnum.A, record.getTestEnum());
        Assert.assertEquals(TestEnum.B, record.getTestEnumUnion());
        Assert.assertEquals(TestEnum.C, record.getTestEnumArray().get(0));
        Assert.assertEquals(TestEnum.D, record.getTestEnumUnionArray().get(0));
    }

    @Test(expected = FastDeserializerGeneratorException.class)
    public void shouldNotReadStrippedEnum() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream(
                "/schema/fastserdetestoldextendedenum.avsc"));
        GenericData.Fixed testFixed = new GenericData.Fixed(oldRecordSchema.getField("testFixed").schema(),
                new byte[] { 0x01 });
        GenericData.Record subRecord = new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema()
                .getTypes().get(1));
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        oldRecord.put("testInt", 1);
        oldRecord.put("testLong", 1l);
        oldRecord.put("testDouble", 1.0);
        oldRecord.put("testFloat", 1.0f);
        oldRecord.put("testBoolean", true);
        oldRecord.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        oldRecord.put("testString", "aaa");
        oldRecord.put("testFixed", testFixed);
        oldRecord.put("testFixedUnion", testFixed);
        oldRecord.put("testFixedArray", Arrays.asList(testFixed));
        oldRecord.put("testFixedUnionArray", Arrays.asList(testFixed));
        oldRecord.put("testEnum", new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(), "F"));
        oldRecord.put("testEnumArray",
                Arrays.asList(new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(), "F")));
        oldRecord.put("testEnumUnionArray", Collections.emptyList());

        oldRecord.put("subRecordUnion", subRecord);
        oldRecord.put("subRecord", subRecord);

        oldRecord.put("recordsArray", Collections.emptyList());
        oldRecord.put("recordsArrayMap", Collections.emptyList());
        oldRecord.put("recordsMap", Collections.emptyMap());
        oldRecord.put("recordsMapArray", Collections.emptyMap());

        // when
        TestRecord record = decodeRecord(TestRecord.getClassSchema(), oldRecordSchema,
                genericDataAsDecoder(oldRecord));
    }

    @Test
    public void shouldReadSubRecordField() {
        // given
        TestRecord record = emptyTestRecord();
        SubRecord subRecord = new SubRecord();
        subRecord.put("subField", "abc");

        record.put("subRecordUnion", subRecord);
        record.put("subRecord", subRecord);

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals("abc",
                record.getSubRecordUnion().getSubField());
        Assert.assertEquals("abc",
                record.getSubRecord().getSubField());
    }

    @Test
    public void shouldReadSubRecordCollectionsField() {

        // given
        TestRecord record = emptyTestRecord();
        SubRecord subRecord = new SubRecord();
        subRecord.put("subField", "abc");

        List<SubRecord> recordsArray = new ArrayList<>();
        recordsArray.add(subRecord);
        record.put("recordsArray", recordsArray);
        record.put("recordsArrayUnion", recordsArray);
        Map<String, SubRecord> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecord);
        record.put("recordsMap", recordsMap);
        record.put("recordsMapUnion", recordsMap);

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals("abc",
                record.getRecordsArray().get(0).getSubField());
        Assert.assertEquals("abc",
                record.getRecordsArrayUnion().get(0).getSubField());
        Assert.assertEquals("abc",
                record.getRecordsMap().get("1").getSubField());
        Assert.assertEquals("abc",
                record.getRecordsMapUnion().get("1").getSubField());
    }

    @Test
    public void shouldReadSubRecordComplexCollectionsField() {
        // given
        TestRecord record = emptyTestRecord();
        SubRecord subRecord = new SubRecord();
        subRecord.put("subField", "abc");

        List<Map<String, SubRecord>> recordsArrayMap = new ArrayList<>();
        Map<String, SubRecord> recordMap = new HashMap<>();
        recordMap.put("1", subRecord);
        recordsArrayMap.add(recordMap);

        record.put("recordsArrayMap", recordsArrayMap);
        record.put("recordsArrayMapUnion", recordsArrayMap);

        Map<String, List<SubRecord>> recordsMapArray = new HashMap<>();
        List<SubRecord> recordList = new ArrayList<>();
        recordList.add(subRecord);
        recordsMapArray.put("1", recordList);

        record.put("recordsMapArray", recordsMapArray);
        record.put("recordsMapArrayUnion", recordsMapArray);

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals("abc", record.getRecordsArrayMap().get(0).get("1").getSubField());
        Assert.assertEquals("abc",
                record.getRecordsMapArray().get("1").get(0).getSubField());
        Assert.assertEquals("abc",
                record.getRecordsArrayMapUnion().get(0).get("1").getSubField());
        Assert.assertEquals("abc",
                record.getRecordsMapArrayUnion().get("1").get(0).getSubField());

    }

    @Test
    public void shouldReadAliasedField() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));
        GenericData.Record subRecord = new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema()
                .getTypes().get(1));
        GenericData.EnumSymbol testEnum = new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(),
                "A");
        GenericData.Fixed testFixed = new GenericData.Fixed(oldRecordSchema.getField("testFixed").schema(),
                new byte[] { 0x01 });
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        oldRecord.put("testInt", 1);
        oldRecord.put("testLong", 1l);
        oldRecord.put("testDouble", 1.0);
        oldRecord.put("testFloat", 1.0f);
        oldRecord.put("testBoolean", true);
        oldRecord.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        oldRecord.put("testString", "aaa");
        oldRecord.put("testFixed", testFixed);
        oldRecord.put("testFixedUnion", testFixed);
        oldRecord.put("testFixedArray", Arrays.asList(testFixed));
        oldRecord.put("testFixedUnionArray", Arrays.asList(testFixed));
        oldRecord.put("testEnum", testEnum);
        oldRecord.put("testEnumUnion", testEnum);
        oldRecord.put("testEnumArray", Arrays.asList(testEnum));
        oldRecord.put("testEnumUnionArray", Arrays.asList(testEnum));

        oldRecord.put("subRecordUnion", subRecord);
        oldRecord.put("subRecord", subRecord);

        oldRecord.put("recordsArray", Collections.emptyList());
        oldRecord.put("recordsArrayMap", Collections.emptyList());
        oldRecord.put("recordsMap", Collections.emptyMap());
        oldRecord.put("recordsMapArray", Collections.emptyMap());

        oldRecord.put("testStringAlias", "abc");

        // when
        TestRecord record = decodeRecord(TestRecord.getClassSchema(), oldRecordSchema,
                genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals("abc", record.getTestStringUnion());
    }

    @Test
    public void shouldSkipRemovedField() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/fastserdetestold.avsc"));

        GenericData.Record subRecord = new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema()
                .getTypes().get(1));
        GenericData.EnumSymbol testEnum = new GenericData.EnumSymbol(oldRecordSchema.getField("testEnum").schema(),
                "A");
        GenericData.Fixed testFixed = new GenericData.Fixed(oldRecordSchema.getField("testFixed").schema(),
                new byte[] { 0x01 });
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        oldRecord.put("testInt", 1);
        oldRecord.put("testLong", 1l);
        oldRecord.put("testDouble", 1.0);
        oldRecord.put("testFloat", 1.0f);
        oldRecord.put("testBoolean", true);
        oldRecord.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        oldRecord.put("testString", "aaa");
        oldRecord.put("testStringAlias", "abc");
        oldRecord.put("removedField", "def");
        oldRecord.put("testFixed", testFixed);
        oldRecord.put("testEnum", testEnum);

        subRecord.put("subField", "abc");
        subRecord.put("removedField", "def");
        subRecord.put("anotherField", "ghi");

        oldRecord.put("subRecordUnion", subRecord);
        oldRecord.put("subRecord", subRecord);
        oldRecord.put("recordsArray", Arrays.asList(subRecord));
        Map<String, GenericData.Record> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecord);
        oldRecord.put("recordsMap", recordsMap);

        oldRecord.put("testFixedArray", Collections.emptyList());
        oldRecord.put("testFixedUnionArray", Collections.emptyList());
        oldRecord.put("testEnumArray", Collections.emptyList());
        oldRecord.put("testEnumUnionArray", Collections.emptyList());
        oldRecord.put("recordsArrayMap", Collections.emptyList());
        oldRecord.put("recordsMapArray", Collections.emptyMap());

        // when
        TestRecord record = decodeRecord(TestRecord.getClassSchema(), oldRecordSchema,
                genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals("abc", record.getTestStringUnion());
        Assert.assertEquals(TestEnum.A, record.getTestEnum());
        Assert.assertEquals("ghi", record.getSubRecordUnion().getAnotherField());
        Assert.assertEquals("ghi", record.getRecordsArray().get(0).getAnotherField());
        Assert.assertEquals("ghi", record.getRecordsMap().get("1").getAnotherField());
    }

    @Test
    public void shouldReadMultipleChoiceUnion() {
        // given
        TestRecord record = emptyTestRecord();
        SubRecord subRecord = new SubRecord();
        subRecord.put("subField", "abc");
        record.put("union", subRecord);

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals("abc", ((SubRecord) record.getUnion()).getSubField());

        // given
        record.put("union", "abc");

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals("abc", record.getUnion());

        // given
        record.put("union", 1);

        // when
        record = decodeRecord(TestRecord.getClassSchema(), TestRecord.getClassSchema(), specificDataAsDecoder(record));

        // then
        Assert.assertEquals(1, record.getUnion());
    }

    @Test
    public void shouldReadArrayOfRecords() {
        // given
        Schema arrayRecordSchema = Schema.createArray(TestRecord.getClassSchema());

        TestRecord testRecord = emptyTestRecord();
        testRecord.put("testStringUnion", "abc");

        List<TestRecord> recordsArray = new ArrayList<>();
        recordsArray.add(testRecord);
        recordsArray.add(testRecord);

        // when
        List<TestRecord> array = decodeRecord(arrayRecordSchema, arrayRecordSchema,
                specificDataAsDecoder(recordsArray, arrayRecordSchema));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("testStringUnion"));
        Assert.assertEquals("abc", array.get(1).get("testStringUnion"));

        // given
        testRecord = emptyTestRecord();
        testRecord.put("testStringUnion", "abc");

        arrayRecordSchema = Schema.createArray(createUnionSchema(TestRecord
                .getClassSchema()));

        recordsArray = new ArrayList<>();
        recordsArray.add(testRecord);
        recordsArray.add(testRecord);

        // when
        array = decodeRecord(arrayRecordSchema, arrayRecordSchema,
                specificDataAsDecoder(recordsArray, arrayRecordSchema));

        // then
        Assert.assertEquals(2, array.size());
        Assert.assertEquals("abc", array.get(0).get("testStringUnion"));
        Assert.assertEquals("abc", array.get(1).get("testStringUnion"));
    }

    @Test
    public void shouldReadMapOfRecords() {
        // given
        Schema mapRecordSchema = Schema.createMap(TestRecord.getClassSchema());

        TestRecord testRecord = emptyTestRecord();
        testRecord.put("testStringUnion", "abc");

        Map<String, TestRecord> recordsMap = new HashMap<>();
        recordsMap.put("1", testRecord);
        recordsMap.put("2", testRecord);

        // when
        Map<String, TestRecord> map = decodeRecord(mapRecordSchema, mapRecordSchema,
                specificDataAsDecoder(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get("1").get("testStringUnion"));
        Assert.assertEquals("abc", map.get("2").get("testStringUnion"));

        // given
        mapRecordSchema = Schema.createMap(FastSerdeTestsSupport.createUnionSchema(TestRecord
                .getClassSchema()));

        testRecord = emptyTestRecord();
        testRecord.put("testStringUnion", "abc");

        recordsMap = new HashMap<>();
        recordsMap.put("1", testRecord);
        recordsMap.put("2", testRecord);

        // when
        map = decodeRecord(mapRecordSchema, mapRecordSchema, specificDataAsDecoder(recordsMap, mapRecordSchema));

        // then
        Assert.assertEquals(2, map.size());
        Assert.assertEquals("abc", map.get("1").get("testStringUnion"));
        Assert.assertEquals("abc", map.get("2").get("testStringUnion"));
    }

    @Test
    public void testNullElementMap() {
        // given
        Schema mapRecordSchema = Schema.createMap(Schema.createUnion(
                Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));

        Map<String, Object> records = new HashMap<>();
        records.put("0", "0");
        records.put("1", null);
        records.put("2", 2);

        // when
        Map<String, Object> map = decodeRecord(mapRecordSchema, mapRecordSchema,
                specificDataAsDecoder(records, mapRecordSchema));

        // then
        Assert.assertEquals(3, map.size());
        Assert.assertEquals("0", map.get("0"));
        Assert.assertNull(map.get("1"));
        Assert.assertEquals(2, map.get("2"));
    }

    @Test
    public void testNullElementArray() {
        // given
        Schema arrayRecordSchema = Schema.createArray(Schema.createUnion(
                Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));

        List<Object> records = new ArrayList<>();
        records.add("0");
        records.add(null);
        records.add(2);

        // when
        List<Object> array = decodeRecord(arrayRecordSchema, arrayRecordSchema,
                specificDataAsDecoder(records, arrayRecordSchema));

        // then
        Assert.assertEquals(3, array.size());
        Assert.assertEquals("0", array.get(0));
        Assert.assertNull(array.get(1));
        Assert.assertEquals(2, array.get(2));
    }

    @SuppressWarnings("unchecked")
    private <T> T decodeRecord(Schema readerSchema, Schema writerSchema,
            Decoder decoder) {
        FastDeserializer<T> deserializer = new FastSpecificDeserializerGenerator(writerSchema,
                readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static TestRecord emptyTestRecord() {
        TestRecord record = new TestRecord();

        record.put("testFixed", new TestFixed(new byte[] { 0x01 }));
        record.put("testFixedArray", Collections.EMPTY_LIST);
        record.put("testFixedUnionArray", Arrays.asList(new TestFixed(new byte[] { 0x01 })));

        record.put("testEnum", TestEnum.A);
        record.put("testEnumArray", Collections.EMPTY_LIST);
        record.put("testEnumUnionArray", Arrays.asList(TestEnum.A));
        record.put("subRecord", new SubRecord());

        record.put("recordsArray", Collections.emptyList());
        record.put("recordsArrayMap", Collections.emptyList());
        record.put("recordsMap", Collections.emptyMap());
        record.put("recordsMapArray", Collections.emptyMap());

        record.put("testInt", 1);
        record.put("testLong", 1l);
        record.put("testDouble", 1.0);
        record.put("testFloat", 1.0f);
        record.put("testBoolean", true);
        record.put("testString", "aaa");
        record.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));

        return record;
    }
}
