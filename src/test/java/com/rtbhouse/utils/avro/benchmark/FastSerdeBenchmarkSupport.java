package com.rtbhouse.utils.avro.benchmark;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

public final class FastSerdeBenchmarkSupport {

    public static final String NAMESPACE = "com.rtbhouse.utils.generated.avro.benchmark";

    private FastSerdeBenchmarkSupport() {
    }

    public static String getRandomString() {
        return RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(2, 40));
    }

    public static Integer getRandomInteger() {
        return RandomUtils.nextInt(0, Integer.MAX_VALUE);
    }

    public static Long getRandomLong() {
        return RandomUtils.nextLong(0l, Long.MAX_VALUE);
    }

    public static Double getRandomDouble() {
        return RandomUtils.nextDouble(0, Double.MAX_VALUE);
    }

    public static Float getRandomFloat() {
        return RandomUtils.nextFloat(0f, Float.MAX_VALUE);
    }

    public static Boolean getRandomBoolean() {
        return RandomUtils.nextInt(0, 10) % 2 == 0 ? Boolean.TRUE : Boolean.FALSE;
    }

    public static ByteBuffer getRandomBytes() {
        return ByteBuffer.wrap(RandomUtils.nextBytes(RandomUtils.nextInt(1, 25)));
    }

    public static List<String> getRandomStringList() {
        return Stream.generate(FastSerdeBenchmarkSupport::getRandomString).limit(RandomUtils.nextInt(1, 10))
                .collect(Collectors.toList());
    }

    public static <T extends SpecificRecord> T toSpecificRecord(GenericData.Record record) throws IOException {
        GenericDatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(record.getSchema());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        Encoder binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null);
        datumWriter.write(record, binaryEncoder);
        binaryEncoder.flush();

        SpecificDatumReader<T> datumReader = new SpecificDatumReader<>(record.getSchema());
        return datumReader.read(null, DecoderFactory.get().binaryDecoder(baos.toByteArray(), null));
    }

    public static Schema generateRandomRecordSchema(String name, int depth, int minNestedRecords, int maxNestedRecords,
            int fieldsNumber) {

        int nestedRecords = 0;
        if (depth != 0) {
            nestedRecords = RandomUtils.nextInt(minNestedRecords, maxNestedRecords + 1);
        }

        final Schema.Type[] types = new Schema.Type[] { Schema.Type.BOOLEAN, Schema.Type.INT, Schema.Type.LONG,
                Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BYTES, Schema.Type.STRING, Schema.Type.FIXED,
                Schema.Type.ENUM, Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION, Schema.Type.UNION, Schema.Type.UNION,
                Schema.Type.UNION };

        final Schema schema = Schema.createRecord(name, null, NAMESPACE, false);

        List<Schema.Field> fields = new ArrayList<>();
        for (int i = 0; i < fieldsNumber; i++) {
            if (i < nestedRecords) {
                fields.add(new Schema.Field(RandomStringUtils.randomAlphabetic(10),
                        generateRandomRecordSchema("NestedRecord" + RandomStringUtils.randomAlphabetic(5), depth - 1,
                                minNestedRecords, maxNestedRecords, fieldsNumber),
                        null, null, Schema.Field.Order.ASCENDING));
            } else {
                final Schema.Type type = types[RandomUtils.nextInt(0, types.length)];
                switch (type) {
                case ENUM:
                    fields.add(
                            new Schema.Field(RandomStringUtils.randomAlphabetic(10), generateRandomEnumSchema(), null,
                                    null, Schema.Field.Order.ASCENDING));
                    break;
                case FIXED:
                    fields.add(
                            new Schema.Field(RandomStringUtils.randomAlphabetic(10), generateRandomFixedSchema(), null,
                                    null, Schema.Field.Order.ASCENDING));
                    break;
                case UNION:
                    fields.add(
                            new Schema.Field(RandomStringUtils.randomAlphabetic(10), generateRandomUnionSchema(), null,
                                    null, Schema.Field.Order.ASCENDING));
                    break;
                case ARRAY:
                    fields.add(
                            new Schema.Field(RandomStringUtils.randomAlphabetic(10), generateRandomArraySchema(), null,
                                    null, Schema.Field.Order.ASCENDING));
                    break;
                case MAP:
                    fields.add(
                            new Schema.Field(RandomStringUtils.randomAlphabetic(10), generateRandomMapSchema(), null,
                                    null, Schema.Field.Order.ASCENDING));
                    break;
                default:
                    fields.add(new Schema.Field(RandomStringUtils.randomAlphabetic(10),
                            Schema.create(type), null, null, Schema.Field.Order.ASCENDING));
                }
            }
        }

        Collections.shuffle(fields);
        schema.setFields(fields);

        return schema;
    }

    public static Schema generateRandomUnionSchema() {
        final Schema.Type[] types = new Schema.Type[] { Schema.Type.BOOLEAN, Schema.Type.INT, Schema.Type.LONG,
                Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BYTES, Schema.Type.STRING, Schema.Type.FIXED,
                Schema.Type.ENUM };

        final Schema.Type type = types[RandomUtils.nextInt(0, types.length)];
        Schema unionSchema = null;
        switch (type) {
        case ENUM:
            unionSchema = Schema
                    .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), generateRandomEnumSchema()));
            break;
        case FIXED:
            unionSchema = Schema
                    .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), generateRandomFixedSchema()));
            break;
        default:
            unionSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(type)));
        }

        return unionSchema;
    }

    public static Schema generateRandomArraySchema() {
        final Schema.Type[] types = new Schema.Type[] { Schema.Type.BOOLEAN, Schema.Type.INT, Schema.Type.LONG,
                Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BYTES, Schema.Type.STRING, Schema.Type.FIXED,
                Schema.Type.ENUM };

        final Schema.Type type = types[RandomUtils.nextInt(0, types.length)];
        Schema arraySchema = null;
        switch (type) {
        case ENUM:
            arraySchema = Schema
                    .createArray(generateRandomEnumSchema());
            break;
        case FIXED:
            arraySchema = Schema
                    .createArray(generateRandomFixedSchema());
            break;
        default:
            arraySchema = Schema.createArray(Schema.create(type));
        }

        return arraySchema;
    }

    public static Schema generateRandomMapSchema() {
        final Schema.Type[] types = new Schema.Type[] { Schema.Type.BOOLEAN, Schema.Type.INT, Schema.Type.LONG,
                Schema.Type.DOUBLE, Schema.Type.FLOAT, Schema.Type.BYTES, Schema.Type.STRING, Schema.Type.FIXED,
                Schema.Type.ENUM };

        final Schema.Type type = types[RandomUtils.nextInt(0, types.length)];
        Schema mapSchema = null;
        switch (type) {
        case ENUM:
            mapSchema = Schema
                    .createMap(generateRandomEnumSchema());
            break;
        case FIXED:
            mapSchema = Schema
                    .createMap(generateRandomFixedSchema());
            break;
        default:
            mapSchema = Schema.createMap(Schema.create(type));
        }

        return mapSchema;
    }

    public static Schema generateRandomEnumSchema() {
        return Schema.createEnum("Enum" + RandomStringUtils.randomAlphabetic(5), null, NAMESPACE,
                getRandomStringList());
    }

    public static Schema generateRandomFixedSchema() {
        return Schema.createFixed("Fixed" + RandomStringUtils.randomAlphabetic(5), null, NAMESPACE,
                RandomUtils.nextInt(1, 10));
    }

    public static GenericData.Record generateRandomRecordData(Schema schema) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new IllegalArgumentException("input schema must be a record schema");
        }

        final GenericData.Record record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            switch (field.schema().getType()) {
            case BOOLEAN:
                record.put(field.pos(), getRandomBoolean());
                break;
            case INT:
                record.put(field.pos(), getRandomInteger());
                break;
            case LONG:
                record.put(field.pos(), getRandomLong());
                break;
            case DOUBLE:
                record.put(field.pos(), getRandomDouble());
                break;
            case FLOAT:
                record.put(field.pos(), getRandomFloat());
                break;
            case BYTES:
                record.put(field.pos(), getRandomBytes());
                break;
            case STRING:
                record.put(field.pos(), getRandomString());
                break;
            case RECORD:
                record.put(field.pos(), generateRandomRecordData(field.schema()));
                break;
            case ENUM:
                record.put(field.pos(), generateRandomEnumSymbol(field.schema()));
                break;
            case FIXED:
                record.put(field.pos(), generateRandomFixed(field.schema()));
                break;
            case UNION:
                record.put(field.pos(), generateRandomUnion(field.schema()));
                break;
            case ARRAY:
                record.put(field.pos(), generateRandomArray(field.schema()));
                break;
            case MAP:
                record.put(field.pos(), generateRandomMap(field.schema()));
                break;
            }
        }

        return record;
    }

    public static GenericData.EnumSymbol generateRandomEnumSymbol(Schema schema) {
        if (!Schema.Type.ENUM.equals(schema.getType())) {
            throw new IllegalArgumentException("input schema must be an enum schema");
        }

        return new GenericData.EnumSymbol(schema,
                schema.getEnumSymbols().get(RandomUtils.nextInt(0, schema.getEnumSymbols().size())));
    }

    public static GenericData.Fixed generateRandomFixed(Schema schema) {
        if (!Schema.Type.FIXED.equals(schema.getType())) {
            throw new IllegalArgumentException("input schema must be an fixed schema");
        }

        return new GenericData.Fixed(schema,
                RandomUtils.nextBytes(schema.getFixedSize()));
    }

    public static Object generateRandomUnion(Schema schema) {
        if (!Schema.Type.UNION.equals(schema.getType())) {
            throw new IllegalArgumentException("input schema must be an union schema");
        }
        Object unionData = null;
        switch (schema.getTypes().get(1).getType()) {
        case BOOLEAN:
            unionData = getRandomBoolean();
            break;
        case INT:
            unionData = getRandomInteger();
            break;
        case LONG:
            unionData = getRandomLong();
            break;
        case DOUBLE:
            unionData = getRandomDouble();
            break;
        case FLOAT:
            unionData = getRandomFloat();
            break;
        case BYTES:
            unionData = getRandomBytes();
            break;
        case STRING:
            unionData = getRandomString();
            break;
        case RECORD:
            unionData = generateRandomRecordData(schema.getTypes().get(1));
            break;
        case ENUM:
            unionData = generateRandomEnumSymbol(schema.getTypes().get(1));
            break;
        case FIXED:
            unionData = generateRandomFixed(schema.getTypes().get(1));
            break;
        case UNION:
            unionData = generateRandomUnion(schema.getTypes().get(1));
        }

        return unionData;
    }

    public static GenericData.Array<?> generateRandomArray(Schema schema) {
        if (!Schema.Type.ARRAY.equals(schema.getType())) {
            throw new IllegalArgumentException("input schema must be an array schema");
        }

        int elements = RandomUtils.nextInt(1, 11);
        GenericData.Array<Object> arrayData = new GenericData.Array<>(elements, schema);
        for (int i = 0; i < elements; i++) {
            switch (schema.getElementType().getType()) {
            case BOOLEAN:
                arrayData.add(getRandomBoolean());
                break;
            case INT:
                arrayData.add(getRandomInteger());
                break;
            case LONG:
                arrayData.add(getRandomLong());
                break;
            case DOUBLE:
                arrayData.add(getRandomDouble());
                break;
            case FLOAT:
                arrayData.add(getRandomFloat());
                break;
            case BYTES:
                arrayData.add(getRandomBytes());
                break;
            case STRING:
                arrayData.add(getRandomString());
                break;
            case RECORD:
                arrayData.add(generateRandomRecordData(schema.getElementType()));
                break;
            case ENUM:
                arrayData.add(generateRandomEnumSymbol(schema.getElementType()));
                break;
            case FIXED:
                arrayData.add(generateRandomFixed(schema.getElementType()));
                break;
            }
        }

        return arrayData;
    }

    public static Map<String, ?> generateRandomMap(Schema schema) {
        if (!Schema.Type.MAP.equals(schema.getType())) {
            throw new IllegalArgumentException("input schema must be an map schema");
        }

        int elements = RandomUtils.nextInt(1, 11);
        Map<String, Object> mapData = new HashMap<>();
        for (int i = 0; i < elements; i++) {
            switch (schema.getValueType().getType()) {
            case BOOLEAN:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomBoolean());
                break;
            case INT:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomInteger());
                break;
            case LONG:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomLong());
                break;
            case DOUBLE:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomDouble());
                break;
            case FLOAT:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomFloat());
                break;
            case BYTES:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomBytes());
                break;
            case STRING:
                mapData.put(RandomStringUtils.randomAlphabetic(10), getRandomString());
                break;
            case RECORD:
                mapData.put(RandomStringUtils.randomAlphabetic(10), generateRandomRecordData(schema.getValueType()));
                break;
            case ENUM:
                mapData.put(RandomStringUtils.randomAlphabetic(10), generateRandomEnumSymbol(schema.getValueType()));
                break;
            case FIXED:
                mapData.put(RandomStringUtils.randomAlphabetic(10), generateRandomFixed(schema.getValueType()));
                break;
            }
        }

        return mapData;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.err.println("Usage: FastBenchmarkSupport name depth minNestedRecords maxNestedRecords fieldsNumber");
            System.exit(1);
        }

        String name = args[0];
        int depth = Integer.parseInt(args[1]);
        int minNestedRecords = Integer.parseInt(args[2]);
        int maxNestedRecords = Integer.parseInt(args[3]);
        int fieldsNumber = Integer.parseInt(args[4]);

        Schema recordSchema = generateRandomRecordSchema(name, depth, minNestedRecords, maxNestedRecords, fieldsNumber);
        FileOutputStream avroFileOutputStream = new FileOutputStream(name + ".avsc");
        avroFileOutputStream.write(recordSchema.toString(true).getBytes());
    }

}
