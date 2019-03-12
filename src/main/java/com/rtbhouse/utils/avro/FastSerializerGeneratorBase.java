package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FastSerializerGeneratorBase<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FastSerializerGenerator.class);

    public static final String GENERATED_PACKAGE_NAME = "com.rtbhouse.utils.avro.serialization.generated";
    public static final String GENERATED_SOURCES_PATH = "/com/rtbhouse/utils/avro/serialization/generated/";

    protected JCodeModel codeModel;
    protected JDefinedClass serializerClass;
    protected final Schema schema;
    private File destination;
    private ClassLoader classLoader;
    private String compileClassPath;

    FastSerializerGeneratorBase(Schema schema, File destination, ClassLoader classLoader,
                                String compileClassPath) {
        this.schema = schema;
        this.destination = destination;
        this.classLoader = classLoader;
        this.compileClassPath = compileClassPath;
        codeModel = new JCodeModel();
    }

    public abstract FastSerializer<T> generateSerializer();

    @SuppressWarnings("unchecked")
    protected Class<FastSerializer<T>> compileClass(final String className) throws IOException,
            ClassNotFoundException {
        final OutputStream infoLoggingStream = LoggingOutputStream.infoLoggingStream(LOGGER);
        final OutputStream errorLoggingStream = LoggingOutputStream.errorLoggingStream(LOGGER);
        codeModel.build(destination, new PrintStream(infoLoggingStream));

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new FastSerializerGeneratorException("no system java compiler is available");
        }

        int compileResult;
        if (compileClassPath != null) {
            compileResult = compiler.run(
                    null,
                    infoLoggingStream,
                    errorLoggingStream,
                    "-cp",
                    compileClassPath,
                    destination.getAbsolutePath() + GENERATED_SOURCES_PATH + className + ".java"
            );
        } else {
            compileResult = compiler.run(
                    null,
                    infoLoggingStream,
                    errorLoggingStream,
                    destination.getAbsolutePath() + GENERATED_SOURCES_PATH + className + ".java");
        }

        if (compileResult != 0) {
            throw new FastSerializerGeneratorException("unable to compile: " + className);
        }

        return (Class<FastSerializer<T>>) classLoader.loadClass(GENERATED_PACKAGE_NAME + "."
                + className);
    }

    public static String getClassName(Schema schema, String description) {
        final Integer schemaId = Math.abs(getSchemaId(schema));
        if (Schema.Type.RECORD.equals(schema.getType())) {
            return schema.getName() + description + "Serializer"
                    + "_" + schemaId;
        } else if (Schema.Type.ARRAY.equals(schema.getType())) {
            return "Array" + description + "Serializer"
                    + "_" + schemaId;
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            return "Map" + description + "Serializer"
                    + "_" + schemaId;
        }
        throw new FastSerializerGeneratorException("Unsupported return type: " + schema.getType());
    }

    protected static String getVariableName(String name) {
        return StringUtils.uncapitalize(name) + nextRandomInt();
    }

    private static final Map<Schema, Integer> SCHEMA_IDS_CACHE = new ConcurrentHashMap<>();
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    public static int getSchemaId(Schema schema) {
        Integer schemaId = SCHEMA_IDS_CACHE.get(schema);
        if (schemaId == null) {
            String schemaString = SchemaNormalization.toParsingForm(schema);
            schemaId = HASH_FUNCTION.hashString(schemaString, Charsets.UTF_8).asInt();
            SCHEMA_IDS_CACHE.put(schema, schemaId);
        }

        return schemaId;
    }

    protected static int nextRandomInt() {
        return Math.abs(ThreadLocalRandom.current().nextInt());
    }
}
