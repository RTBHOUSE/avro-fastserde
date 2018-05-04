package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.genericDataAsDecoder;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.rtbhouse.utils.generated.avro.DefaultsEnum;
import com.rtbhouse.utils.generated.avro.DefaultsFixed;
import com.rtbhouse.utils.generated.avro.DefaultsNewEnum;
import com.rtbhouse.utils.generated.avro.DefaultsSubRecord;
import com.rtbhouse.utils.generated.avro.DefaultsTestRecord;
import com.rtbhouse.utils.generated.avro.OldSubRecord;
import com.rtbhouse.utils.generated.avro.TestRecord;

public class FastDeserializerDefaultsTest {

    private File tempDir;
    private ClassLoader classLoader;

    @Before
    public void prepare() throws Exception {
        Path tempPath = Files.createTempDirectory("generated");
        tempDir = tempPath.toFile();

        classLoader = URLClassLoader.newInstance(new URL[] { tempDir.toURI().toURL() },
                FastDeserializerDefaultsTest.class.getClassLoader());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReadSpecificDefaults() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
        oldSubRecord.put("oldSubField", "testValueOfSubField");
        oldSubRecord.put("fieldToBeRemoved", 33);
        oldRecord.put("oldSubRecord", oldSubRecord);

        // when
        DefaultsTestRecord testRecord = decodeSpecificFast(DefaultsTestRecord.getClassSchema(), oldRecordSchema,
                genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals(oldSubRecord.get("oldSubField"),
                ((OldSubRecord) testRecord.get("oldSubRecord")).get("oldSubField"));
        Assert.assertEquals("defaultOldSubField",
                ((OldSubRecord) testRecord.get("newFieldWithOldSubRecord")).get("oldSubField"));
        Assert.assertEquals(42, (int) testRecord.getTestInt());
        Assert.assertNull(testRecord.getTestIntUnion());
        Assert.assertEquals(9223372036854775807L, (long) testRecord.getTestLong());
        Assert.assertNull(testRecord.getTestLongUnion());
        Assert.assertEquals(3.14d, testRecord.getTestDouble(), 0);
        Assert.assertNull(testRecord.getTestDoubleUnion());
        Assert.assertEquals(3.14f, testRecord.getTestFloat(), 0);
        Assert.assertNull(testRecord.getTestFloatUnion());
        Assert.assertEquals(true, testRecord.getTestBoolean());
        Assert.assertNull(testRecord.getTestBooleanUnion());
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0, 1, 2, 3 }), testRecord.getTestBytes());
        Assert.assertNull(testRecord.getTestBytesUnion());
        Assert.assertEquals("testStringValue", testRecord.getTestString());
        Assert.assertEquals(new URL("http://www.example.com"), testRecord.getTestStringable());
        Assert.assertNull(testRecord.getTestStringUnion());
        Assert.assertEquals(new DefaultsFixed(new byte[] { (byte) 0xFF }), testRecord.getTestFixed());
        Assert.assertNull(testRecord.getTestFixedUnion());
        Assert.assertEquals(Collections.singletonList(new DefaultsFixed(new byte[] { (byte) 0xFA })),
                testRecord.getTestFixedArray());

        List listWithNull = new LinkedList();
        listWithNull.add(null);
        Assert.assertEquals(listWithNull, testRecord.getTestFixedUnionArray());
        Assert.assertEquals(DefaultsEnum.C, testRecord.getTestEnum());
        Assert.assertNull(testRecord.getTestEnumUnion());
        Assert.assertEquals(Collections.singletonList(Collections.singletonList(DefaultsNewEnum.B)),
                testRecord.getTestNewEnumIntUnionArray());
        Assert.assertEquals(Arrays.asList(DefaultsEnum.E, DefaultsEnum.B), testRecord.getTestEnumArray());
        Assert.assertEquals(listWithNull, testRecord.getTestEnumUnionArray());
        Assert.assertNull(testRecord.getSubRecordUnion());
        Assert.assertEquals(DefaultsSubRecord.newBuilder().setSubField("valueOfSubField")
                .setArrayField(Collections.singletonList(DefaultsEnum.A)).build(), testRecord.getSubRecord());
        Assert.assertEquals(Collections.singletonList(DefaultsSubRecord.newBuilder().setSubField("recordArrayValue")
                .setArrayField(Collections.singletonList(DefaultsEnum.A)).build()), testRecord.getRecordArray());
        Assert.assertEquals(listWithNull, testRecord.getRecordUnionArray());

        Map stringableMap = new HashMap();
        stringableMap.put(new URL("http://www.example2.com"), new BigInteger("123"));
        Assert.assertEquals(stringableMap, testRecord.getStringableMap());

        Map recordMap = new HashMap();
        recordMap.put("test", DefaultsSubRecord.newBuilder().setSubField("recordMapValue")
                .setArrayField(Collections.singletonList(DefaultsEnum.A)).build());
        Assert.assertEquals(recordMap, testRecord.getRecordMap());

        Map recordUnionMap = new HashMap();
        recordUnionMap.put("test", null);
        Assert.assertEquals(recordUnionMap, testRecord.getRecordUnionMap());
        Assert.assertEquals(Collections.singletonList(recordUnionMap), testRecord.getRecordUnionMapArray());

        Map recordUnionArrayMap = new HashMap();
        recordUnionArrayMap.put("test", listWithNull);
        Assert.assertEquals(recordUnionArrayMap, testRecord.getRecordUnionArrayMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReadGenericDefaults() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
        oldSubRecord.put("oldSubField", "testValueOfSubField");
        oldSubRecord.put("fieldToBeRemoved", 33);
        oldRecord.put("oldSubRecord", oldSubRecord);

        // when
        GenericRecord testRecord = decodeGenericFast(DefaultsTestRecord.getClassSchema(), oldRecordSchema,
                genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals(oldSubRecord.get("oldSubField"),
                ((GenericData.Record) testRecord.get("oldSubRecord")).get("oldSubField"));
        Assert.assertEquals("defaultOldSubField",
                ((GenericData.Record) testRecord.get("newFieldWithOldSubRecord")).get("oldSubField"));
        Assert.assertEquals(42, (int) testRecord.get("testInt"));
        Assert.assertNull(testRecord.get("testIntUnion"));
        Assert.assertEquals(9223372036854775807L, (long) testRecord.get("testLong"));
        Assert.assertNull(testRecord.get("testLongUnion"));
        Assert.assertEquals(3.14d, (double) testRecord.get("testDouble"), 0);
        Assert.assertNull(testRecord.get("testDoubleUnion"));
        Assert.assertEquals(3.14f, (float) testRecord.get("testFloat"), 0);
        Assert.assertNull(testRecord.get("testFloatUnion"));
        Assert.assertEquals(true, testRecord.get("testBoolean"));
        Assert.assertNull(testRecord.get("testBooleanUnion"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0, 1, 2, 3 }), testRecord.get("testBytes"));
        Assert.assertNull(testRecord.get("testBytesUnion"));
        Assert.assertEquals("testStringValue", testRecord.get("testString"));
        Assert.assertEquals("http://www.example.com", testRecord.get("testStringable"));
        Assert.assertNull(testRecord.get("testStringUnion"));
        Assert.assertEquals(new GenericData.Fixed(DefaultsFixed.getClassSchema(), new byte[] { (byte) 0xFF }),
                testRecord.get("testFixed"));
        Assert.assertNull(testRecord.get("testFixedUnion"));
        Assert.assertEquals(
                Collections.singletonList(
                        new GenericData.Fixed(DefaultsFixed.getClassSchema(), new byte[] { (byte) 0xFA })),
                testRecord.get("testFixedArray"));

        List listWithNull = new LinkedList();
        listWithNull.add(null);
        Assert.assertEquals(listWithNull, testRecord.get("testFixedUnionArray"));
        Assert.assertEquals("C", testRecord.get("testEnum").toString());
        Assert.assertNull(testRecord.get("testEnumUnion"));
        Assert.assertEquals(
                Collections.singletonList(
                        Collections.singletonList(new GenericData.EnumSymbol(DefaultsNewEnum.getClassSchema(), "B"))),
                testRecord.get("testNewEnumIntUnionArray"));
        Assert.assertEquals("E", ((List<GenericData.EnumSymbol>) testRecord.get("testEnumArray")).get(0).toString());
        Assert.assertEquals("B", ((List<GenericData.EnumSymbol>) testRecord.get("testEnumArray")).get(1).toString());
        Assert.assertEquals(listWithNull, testRecord.get("testEnumUnionArray"));
        Assert.assertNull(testRecord.get("subRecordUnion"));
        Assert.assertEquals(newGenericSubRecord("valueOfSubField", null, "A"), testRecord.get("subRecord"));
        Assert.assertEquals(Collections.singletonList(newGenericSubRecord("recordArrayValue", null, "A")),
                testRecord.get("recordArray"));
        Assert.assertEquals(listWithNull, testRecord.get("recordUnionArray"));

        Map stringableMap = new HashMap();
        stringableMap.put("http://www.example2.com", "123");
        Assert.assertEquals(stringableMap, testRecord.get("stringableMap"));

        Map recordMap = new HashMap();
        recordMap.put("test", newGenericSubRecord("recordMapValue", null, "A"));
        Assert.assertEquals(recordMap, testRecord.get("recordMap"));

        Map recordUnionMap = new HashMap();
        recordUnionMap.put("test", null);
        Assert.assertEquals(recordUnionMap, testRecord.get("recordUnionMap"));
        Assert.assertEquals(Collections.singletonList(recordUnionMap), testRecord.get("recordUnionMapArray"));

        Map recordUnionArrayMap = new HashMap();
        recordUnionArrayMap.put("test", listWithNull);
        Assert.assertEquals(recordUnionArrayMap, testRecord.get("recordUnionArrayMap"));
    }

    @Test
    public void shouldReadSpecificLikeSlow() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
        oldSubRecord.put("oldSubField", "testValueOfSubField");
        oldSubRecord.put("fieldToBeRemoved", 33);
        oldRecord.put("oldSubRecord", oldSubRecord);

        // when
        DefaultsTestRecord testRecordSlow = decodeSpecificSlow(DefaultsTestRecord.getClassSchema(),
                oldRecordSchema, genericDataAsDecoder(oldRecord));
        DefaultsTestRecord testRecordFast = decodeSpecificFast(DefaultsTestRecord.getClassSchema(),
                oldRecordSchema, genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals(testRecordSlow, testRecordFast);
    }

    @Test
    public void shouldReadGenericLikeSlow() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = parser.parse(this.getClass().getResourceAsStream("/schema/defaultsTestOld.avsc"));
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);
        GenericData.Record oldSubRecord = new GenericData.Record(oldRecordSchema.getField("oldSubRecord").schema());
        oldSubRecord.put("oldSubField", "testValueOfSubField");
        oldSubRecord.put("fieldToBeRemoved", 33);
        oldRecord.put("oldSubRecord", oldSubRecord);

        // when
        GenericRecord testRecordSlow = decodeGenericSlow(DefaultsTestRecord.getClassSchema(),
                oldRecordSchema, genericDataAsDecoder(oldRecord));
        GenericRecord testRecordFast = decodeGenericFast(DefaultsTestRecord.getClassSchema(),
                oldRecordSchema, genericDataAsDecoder(oldRecord));

        // then
        Assert.assertEquals(testRecordSlow, testRecordFast);
    }

    @Test
    public void shouldAddFieldsInMiddleOfSchema() throws IOException {
        // given
        Schema.Parser parser = new Schema.Parser();
        Schema oldRecordSchema = TestRecord.getClassSchema();

        GenericData.Record subRecord = new GenericData.Record(oldRecordSchema.getField("subRecordUnion").schema()
                .getTypes().get(1));
        GenericData.EnumSymbol testEnum = new GenericData.EnumSymbol(
                oldRecordSchema.getField("testEnum").schema(), "A");
        GenericData.Fixed testFixed = new GenericData.Fixed(oldRecordSchema.getField("testFixed").schema(),
                new byte[] { 0x01 });
        GenericData.Record oldRecord = new GenericData.Record(oldRecordSchema);

        oldRecord.put("testInt", 1);
        oldRecord.put("testLong", 1L);
        oldRecord.put("testDouble", 1.0);
        oldRecord.put("testFloat", 1.0f);
        oldRecord.put("testBoolean", true);
        oldRecord.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));
        oldRecord.put("testString", "aaa");
        oldRecord.put("testFixed", testFixed);
        oldRecord.put("testEnum", testEnum);

        subRecord.put("subField", "abc");
        subRecord.put("anotherField", "ghi");

        oldRecord.put("subRecordUnion", subRecord);
        oldRecord.put("subRecord", subRecord);
        oldRecord.put("recordsArray", Collections.singletonList(subRecord));
        Map<String, GenericData.Record> recordsMap = new HashMap<>();
        recordsMap.put("1", subRecord);
        oldRecord.put("recordsMap", recordsMap);

        oldRecord.put("testFixedArray", Collections.emptyList());
        oldRecord.put("testFixedUnionArray", Collections.emptyList());
        oldRecord.put("testEnumArray", Collections.emptyList());
        oldRecord.put("testEnumUnionArray", Collections.emptyList());
        oldRecord.put("recordsArrayMap", Collections.emptyList());
        oldRecord.put("recordsMapArray", Collections.emptyMap());

        Schema newRecordSchema = parser
                .parse(this.getClass().getResourceAsStream("/schema/defaultsTestSubrecord.avsc"));
        // when
        GenericRecord record = decodeGenericFast(newRecordSchema, oldRecordSchema, genericDataAsDecoder(oldRecord));

        // then
        GenericData.Record newSubRecord = new GenericData.Record(newRecordSchema.getField("subRecordUnion")
                .schema().getTypes().get(1));
        newSubRecord.put("subField", "abc");
        newSubRecord.put("anotherField", "ghi");
        newSubRecord.put("newSubField", "newSubFieldValue");
        recordsMap.put("1", newSubRecord);

        Assert.assertEquals("newSubFieldValue",
                ((GenericRecord) record.get("subRecordUnion")).get("newSubField").toString());
        Assert.assertEquals("newFieldValue", record.get("newField").toString());
        Assert.assertEquals(1, record.get("testInt"));
        Assert.assertEquals(1L, record.get("testLong"));
        Assert.assertEquals(1.0, record.get("testDouble"));
        Assert.assertEquals(1.0f, record.get("testFloat"));
        Assert.assertEquals(true, record.get("testBoolean"));
        Assert.assertEquals(ByteBuffer.wrap(new byte[] { 0x01, 0x02 }), record.get("testBytes"));
        Assert.assertEquals("aaa", record.get("testString"));
        Assert.assertEquals(testFixed, record.get("testFixed"));
        Assert.assertEquals(testEnum, record.get("testEnum"));
        Assert.assertEquals(newSubRecord, record.get("subRecordUnion"));

        Assert.assertEquals(Collections.singletonList(newSubRecord), record.get("recordsArray"));
        Assert.assertEquals(recordsMap, record.get("recordsMap"));
        Assert.assertEquals(Collections.emptyList(), record.get("testFixedArray"));
        Assert.assertEquals(Collections.emptyList(), record.get("testFixedUnionArray"));
        Assert.assertEquals(Collections.emptyList(), record.get("testEnumArray"));
        Assert.assertEquals(Collections.emptyList(), record.get("testEnumUnionArray"));
        Assert.assertEquals(Collections.emptyList(), record.get("recordsArrayMap"));
        Assert.assertEquals(Collections.emptyMap(), record.get("recordsMapArray"));

    }

    private GenericRecord newGenericSubRecord(String subField, String anotherField, String arrayEnumValue) {
        GenericRecordBuilder builder = new GenericRecordBuilder(DefaultsSubRecord.getClassSchema());
        builder.set("subField", subField);
        builder.set("anotherField", anotherField);
        builder.set("arrayField",
                Collections.singletonList(new GenericData.EnumSymbol(DefaultsEnum.getClassSchema(), arrayEnumValue)));
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private <T> T decodeSpecificSlow(Schema readerSchema, Schema writerSchema, Decoder decoder) {
        org.apache.avro.io.DatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema, readerSchema);
        try {
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    @SuppressWarnings("unchecked")
    private GenericRecord decodeGenericSlow(Schema readerSchema, Schema writerSchema, Decoder decoder) {
        org.apache.avro.io.DatumReader<GenericData> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        try {
            return (GenericRecord) datumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    @SuppressWarnings("unchecked")
    private <T> T decodeSpecificFast(Schema readerSchema, Schema writerSchema, Decoder decoder) {
        FastDeserializer<T> deserializer = new FastSpecificDeserializerGenerator(writerSchema,
                readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private GenericRecord decodeGenericFast(Schema readerSchema, Schema writerSchema, Decoder decoder) {
        FastDeserializer<GenericRecord> deserializer = new FastGenericDeserializerGenerator(writerSchema,
                readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
