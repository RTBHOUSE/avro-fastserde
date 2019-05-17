package com.rtbhouse.utils.avro;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.codehaus.jackson.node.NullNode;

public final class FastSerdeTestsSupport {

    public static final String NAMESPACE = "com.rtbhouse.utils.generated.avro";

    private FastSerdeTestsSupport() {
    }

    public static Schema createRecord(String name, Schema.Field... fields) {
        Schema schema = Schema.createRecord(name, name, NAMESPACE, false);
        schema.setFields(Arrays.asList(fields));

        return schema;
    }

    public static Schema.Field createField(String name, Schema schema) {
        return new Schema.Field(name, schema, "", (Object) null, Schema.Field.Order.ASCENDING);
    }

    public static Schema.Field createUnionField(String name, Schema... schemas) {
        List<Schema> typeList = new ArrayList<>();
        typeList.add(Schema.create(Schema.Type.NULL));
        typeList.addAll(Arrays.asList(schemas));

        Schema unionSchema = Schema.createUnion(typeList);
        return new Schema.Field(name, unionSchema, null, Schema.Field.Order.ASCENDING);
    }

    public static Schema.Field createPrimitiveFieldSchema(String name, Schema.Type type) {
        return new Schema.Field(name, Schema.create(type), null, (Object) null);
    }

    public static Schema.Field createPrimitiveUnionFieldSchema(String name, Schema.Type... types) {
        List<Schema> typeList = new ArrayList<>();
        typeList.add(Schema.create(Schema.Type.NULL));
        typeList.addAll(Arrays.stream(types).map(Schema::create).collect(Collectors.toList()));

        Schema unionSchema = Schema.createUnion(typeList);
        return new Schema.Field(name, unionSchema, null, (Object) null, Schema.Field.Order.ASCENDING);
    }

    public static Schema.Field createArrayFieldSchema(String name, Schema elementType, String... aliases) {
        return addAliases(new Schema.Field(name, Schema.createArray(elementType), null, (Object) null,
            Schema.Field.Order.ASCENDING), aliases);
    }

    public static Schema.Field createMapFieldSchema(String name, Schema valueType, String... aliases) {
        return addAliases(new Schema.Field(name, Schema.createMap(valueType), null, (Object) null,
            Schema.Field.Order.ASCENDING), aliases);
    }

    public static Schema createFixedSchema(String name, int size) {
        return Schema.createFixed(name, "", NAMESPACE, size);
    }

    public static Schema createEnumSchema(String name, String[] ordinals) {
        return Schema.createEnum(name, "", NAMESPACE, Arrays.asList(ordinals));
    }

    public static Schema createUnionSchema(Schema... schemas) {
        List<Schema> typeList = new ArrayList<>();
        typeList.add(Schema.create(Schema.Type.NULL));
        typeList.addAll(Arrays.asList(schemas));

        return Schema.createUnion(typeList);
    }

    public static Schema.Field addAliases(Schema.Field field, String... aliases) {
        if (aliases != null) {
            Arrays.asList(aliases).forEach(field::addAlias);
        }

        return field;
    }

    public static <T extends GenericContainer> Decoder serializeGeneric(T data) {
        return serializeGeneric(data, data.getSchema());
    }

    public static <T> Decoder serializeGeneric(T data, Schema schema) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);

        try {
            GenericDatumWriter<T> writer = new GenericDatumWriter<>(schema);
            writer.write(data, binaryEncoder);
            binaryEncoder.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
    }

    public static <T> T deserializeGeneric(Schema schema, Decoder decoder) {
        GenericDatumReader<T> datumReader = new GenericDatumReader<>(schema);
        try {
            return datumReader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends SpecificRecord> Decoder serializeSpecific(T record) {
        return serializeSpecific(record, record.getSchema());
    }

    public static <T> Decoder serializeSpecific(T record, Schema schema) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);

        try {
            SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
            writer.write(record, binaryEncoder);
            binaryEncoder.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
    }


    public static <T> T deserializeSpecific(Schema writerSchema, Decoder decoder) {
        SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(writerSchema);
        try {
            return datumReader.read(null, decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static com.rtbhouse.utils.generated.avro.TestRecord emptyTestRecord() {
        com.rtbhouse.utils.generated.avro.TestRecord record = new com.rtbhouse.utils.generated.avro.TestRecord();

        record.put("testFixed", new com.rtbhouse.utils.generated.avro.TestFixed(new byte[] { 0x01 }));
        record.put("testFixedArray", Collections.EMPTY_LIST);
        record.put("testFixedUnionArray", Collections.singletonList(new com.rtbhouse.utils.generated.avro.TestFixed(new byte[] { 0x01 })));

        record.put("testEnum", com.rtbhouse.utils.generated.avro.TestEnum.A);
        record.put("testEnumArray", Collections.EMPTY_LIST);
        record.put("testEnumUnionArray", Collections.singletonList(com.rtbhouse.utils.generated.avro.TestEnum.A));
        record.put("subRecord", new com.rtbhouse.utils.generated.avro.SubRecord());

        record.put("recordsArray", Collections.emptyList());
        record.put("recordsArrayMap", Collections.emptyList());
        record.put("recordsMap", Collections.emptyMap());
        record.put("recordsMapArray", Collections.emptyMap());

        record.put("testInt", 1);
        record.put("testLong", 1L);
        record.put("testDouble", 1.0);
        record.put("testFloat", 1.0f);
        record.put("testBoolean", true);
        record.put("testString", "aaa");
        record.put("testBytes", ByteBuffer.wrap(new byte[] { 0x01, 0x02 }));

        return record;
    }
}
