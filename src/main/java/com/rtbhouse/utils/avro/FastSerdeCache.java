package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Fast avro serializer/deserializer cache. Stores generated and already compiled instances of serializers and
 * deserializers for future use.
 */
@SuppressWarnings("unchecked")
public final class FastSerdeCache {

    public static final String GENERATED_CLASSES_DIR = "avro.fast.serde.classes.dir";
    public static final String CLASSPATH = "avro.fast.serde.classpath";
    public static final String CLASSPATH_SUPPLIER = "avro.fast.serde.classpath.supplier";

    private static final Logger LOGGER = Logger.getLogger(FastSerdeCache.class.getName());

    private static volatile FastSerdeCache INSTANCE;

    private final ConcurrentHashMap<String, FastDeserializer<?>> fastSpecificRecordDeserializersCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, FastDeserializer<?>> fastGenericRecordDeserializersCache = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, FastSerializer<?>> fastSpecificRecordSerializersCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, FastSerializer<?>> fastGenericRecordSerializersCache = new ConcurrentHashMap<>();

    private Executor executor;

    private File classesDir;
    private ClassLoader classLoader;

    private Optional<String> compileClassPath;

    /**
     *
     * @param compileClassPathSupplier
     *            custom classpath {@link Supplier}
     */
    public FastSerdeCache(Supplier<String> compileClassPathSupplier) {
        this(compileClassPathSupplier != null ? compileClassPathSupplier.get() : null);
    }

    /**
     *
     * @param executorService
     *            {@link Executor} used by serializer/deserializer compile threads
     * @param compileClassPathSupplier
     *            custom classpath {@link Supplier}
     */
    public FastSerdeCache(Executor executorService, Supplier<String> compileClassPathSupplier) {
        this(executorService, compileClassPathSupplier.get());
    }

    public FastSerdeCache(String compileClassPath) {
        this();
        this.compileClassPath = Optional.ofNullable(compileClassPath);
    }

    /**
     *
     * @param executorService
     *            customized {@link Executor} used by serializer/deserializer compile threads
     * @param compileClassPath
     *            custom classpath as string
     */
    public FastSerdeCache(Executor executorService, String compileClassPath) {
        this(executorService);
        this.compileClassPath = Optional.ofNullable(compileClassPath);
    }

    /**
     *
     * @param executorService
     *            customized {@link Executor} used by serializer/deserializer compile threads
     */
    public FastSerdeCache(Executor executorService) {
        this.executor = executorService != null ? executorService : getDefaultExecutor();

        try {
            Path classesPath;
            if (System.getProperty(GENERATED_CLASSES_DIR) != null) {
                classesPath = Paths.get(System.getProperty(GENERATED_CLASSES_DIR));
                classesDir = classesPath.toFile();
            } else {
                classesPath = Files.createTempDirectory("generated");
                classesDir = classesPath.toFile();
            }

            classLoader = URLClassLoader.newInstance(new URL[] { classesDir.toURI().toURL() },
                    FastSerdeCache.class.getClassLoader());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.compileClassPath = Optional.empty();
    }

    private FastSerdeCache() {
        executor = getDefaultExecutor();

        try {
            Path classesPath;
            if (System.getProperty(GENERATED_CLASSES_DIR) != null) {
                classesPath = Paths.get(System.getProperty(GENERATED_CLASSES_DIR));
                classesDir = classesPath.toFile();
            } else {
                classesPath = Files.createTempDirectory("generated");
                classesDir = classesPath.toFile();
            }

            classLoader = URLClassLoader.newInstance(new URL[] { classesDir.toURI().toURL() },
                    FastSerdeCache.class.getClassLoader());

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.compileClassPath = Optional.empty();
    }

    /**
     * Gets default {@link FastSerdeCache} instance. Default instance classpath can be customized via
     * {@value #CLASSPATH} or {@value #CLASSPATH_SUPPLIER} system properties.
     *
     * @return default {@link FastSerdeCache} instance
     */
    public static FastSerdeCache getDefaultInstance() {
        if (INSTANCE == null) {
            synchronized (FastSerdeCache.class) {
                if (INSTANCE == null) {
                    String classPath = System.getProperty(CLASSPATH);
                    String classpathSupplierClassName = System.getProperty(CLASSPATH_SUPPLIER);
                    if (classpathSupplierClassName != null) {
                        Supplier<String> classpathSupplier = null;
                        try {
                            Class<?> classPathSupplierClass = Class.forName(classpathSupplierClassName);
                            if (Supplier.class.isAssignableFrom(classPathSupplierClass) && String.class
                                    .equals((Class<?>) ((ParameterizedType) classPathSupplierClass
                                            .getGenericSuperclass())
                                                    .getActualTypeArguments()[0])) {

                                classpathSupplier = (Supplier<String>) classPathSupplierClass.newInstance();
                            } else {
                                LOGGER.log(Level.WARNING,
                                        "classpath supplier must be subtype of java.util.function.Supplier: "
                                                + classpathSupplierClassName);
                            }
                        } catch (ReflectiveOperationException e) {
                            LOGGER.log(Level.WARNING,
                                    "unable to instantiate classpath supplier: " + classpathSupplierClassName, e);
                        }
                        INSTANCE = new FastSerdeCache(classpathSupplier);
                    } else if (classPath != null) {
                        INSTANCE = new FastSerdeCache(classPath);
                    } else {
                        INSTANCE = new FastSerdeCache();
                    }
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Generates if needed and returns specific-class aware avro {@link FastDeserializer}.
     *
     * @param writerSchema
     *            {@link Schema} of written data
     * @param readerSchema
     *            {@link Schema} intended to be used during deserialization
     * @return specific-class aware avro {@link FastDeserializer}
     */
    public FastDeserializer<?> getFastSpecificDeserializer(Schema writerSchema,
            Schema readerSchema) {
        String schemaKey = getSchemaKey(writerSchema, readerSchema);
        FastDeserializer<?> deserializer = fastSpecificRecordDeserializersCache.get(schemaKey);

        if (deserializer == null) {
            deserializer = fastSpecificRecordDeserializersCache.putIfAbsent(schemaKey,
                    d -> new SpecificDatumReader<>(writerSchema, readerSchema).read(null, d));
            if (deserializer == null) {
                deserializer = fastSpecificRecordDeserializersCache.get(schemaKey);
                CompletableFuture
                        .supplyAsync(() -> buildSpecificDeserializer(writerSchema, readerSchema), executor).thenAccept(
                                d -> {
                                    fastSpecificRecordDeserializersCache.put(schemaKey, d);
                                });
            }
        }

        return deserializer;
    }

    /**
     * Generates if needed and returns generic-class aware avro {@link FastDeserializer}.
     *
     * @param writerSchema
     *            {@link Schema} of written data
     * @param readerSchema
     *            {@link Schema} intended to be used during deserialization
     * @return generic-class aware avro {@link FastDeserializer}
     */
    public FastDeserializer<?> getFastGenericDeserializer(Schema writerSchema, Schema readerSchema) {
        String schemaKey = getSchemaKey(writerSchema, readerSchema);
        FastDeserializer<?> deserializer = fastGenericRecordDeserializersCache.get(schemaKey);

        if (deserializer == null) {
            deserializer = fastGenericRecordDeserializersCache.putIfAbsent(
                    schemaKey,
                    d -> new GenericDatumReader<>(writerSchema, readerSchema).read(null, d));
            if (deserializer == null) {
                deserializer = fastGenericRecordDeserializersCache.get(schemaKey);
                CompletableFuture
                        .supplyAsync(() -> buildGenericDeserializer(writerSchema, readerSchema), executor).thenAccept(
                                d -> {
                                    fastGenericRecordDeserializersCache.put(schemaKey, d);
                                });
            }
        }
        return deserializer;
    }

    /**
     * Generates if needed and returns specific-class aware avro {@link FastSerializer}.
     *
     * @param schema
     *            {@link Schema} of data to write
     * @return specific-class aware avro {@link FastSerializer}
     */
    public FastSerializer<?> getFastSpecificSerializer(Schema schema) {
        String schemaKey = getSchemaKey(schema, schema);
        FastSerializer<?> serializer = fastSpecificRecordSerializersCache.get(schemaKey);
        if (serializer == null) {
            serializer = fastSpecificRecordSerializersCache.putIfAbsent(
                    schemaKey,
                    (d, e) -> new SpecificDatumWriter<>(schema).write(d, e));
            if (serializer == null) {
                serializer = fastSpecificRecordSerializersCache.get(schemaKey);
                CompletableFuture
                        .supplyAsync(() -> buildSpecificSerializer(schema), executor).thenAccept(
                                s -> {
                                    fastSpecificRecordSerializersCache.put(schemaKey, s);
                                });
            }
        }

        return serializer;
    }

    /**
     * Generates if needed and returns generic-class aware avro {@link FastSerializer}.
     *
     * @param schema
     *            {@link Schema} of data to write
     * @return generic-class aware avro {@link FastSerializer}
     */
    public FastSerializer<?> getFastGenericSerializer(Schema schema) {
        String schemaKey = getSchemaKey(schema, schema);

        FastSerializer<?> serializer = fastGenericRecordSerializersCache.get(schemaKey);
        if (serializer == null) {
            serializer = fastGenericRecordSerializersCache.putIfAbsent(
                    schemaKey,
                    (d, e) -> new GenericDatumWriter<>(schema).write(d, e));
            if (serializer == null) {
                serializer = fastGenericRecordSerializersCache.get(schemaKey);
                CompletableFuture
                        .supplyAsync(() -> buildGenericSerializer(schema), executor).thenAccept(
                                s -> {
                                    fastGenericRecordSerializersCache.put(schemaKey, s);
                                });
            }
        }
        return serializer;
    }

    private String getSchemaKey(Schema writerSchema, Schema readerSchema) {
        return String.valueOf(Math.abs(FastDeserializerGeneratorBase.getSchemaId(writerSchema)))
                + Math.abs(FastDeserializerGeneratorBase.getSchemaId(readerSchema));
    }

    private FastDeserializer<?> buildSpecificDeserializer(Schema writerSchema, Schema readerSchema) {
        try {
            String className = FastDeserializerGeneratorBase.getClassName(writerSchema, readerSchema, "Specific");
            Optional<Path> clazzFile = Files.walk(classesDir.toPath()).filter(p -> p.getFileName()
                    .startsWith(className + ".class")).findFirst();
            if (clazzFile.isPresent()) {
                Class<FastDeserializer<?>> fastDeserializerClass = (Class<FastDeserializer<?>>) classLoader
                        .loadClass(FastDeserializerGeneratorBase.GENERATED_PACKAGE_NAME + "." + className);

                return fastDeserializerClass.newInstance();
            } else {
                FastSpecificDeserializerGenerator<?> generator = new FastSpecificDeserializerGenerator<>(
                        writerSchema, readerSchema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));

                return generator.generateDeserializer();
            }
        } catch (FastDeserializerGeneratorException e) {
            LOGGER.log(Level.WARNING, "deserializer generation exception", e);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "deserializer class instantiation exception", e);
        }

        return d -> new SpecificDatumReader<>(writerSchema, readerSchema).read(null, d);
    }

    private FastDeserializer<?> buildGenericDeserializer(Schema writerSchema, Schema readerSchema) {
        try {
            String className = FastDeserializerGeneratorBase.getClassName(writerSchema, readerSchema,
                    "Generic");
            Optional<Path> clazzFile = Files.walk(classesDir.toPath()).filter(p -> p.getFileName()
                    .startsWith(className + ".class")).findFirst();
            if (clazzFile.isPresent()) {
                Class<FastDeserializer<?>> fastDeserializerClass = (Class<FastDeserializer<?>>) classLoader
                        .loadClass(FastDeserializerGeneratorBase.GENERATED_PACKAGE_NAME + "." + className);

                return fastDeserializerClass.getConstructor(Schema.class).newInstance(readerSchema);
            } else {
                FastGenericDeserializerGenerator<?> generator = new FastGenericDeserializerGenerator<>(
                        writerSchema, readerSchema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));

                return generator.generateDeserializer();
            }

        } catch (FastDeserializerGeneratorException e) {
            LOGGER.log(Level.WARNING, "deserializer generation exception", e);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "deserializer class instantiation exception", e);
        }

        return d -> new GenericDatumReader<>(writerSchema, readerSchema).read(null, d);
    }

    private FastSerializer<?> buildSpecificSerializer(Schema schema) {
        try {
            String className = FastSerializerGeneratorBase.getClassName(schema, "Specific");
            Optional<Path> clazzFile = Files.walk(classesDir.toPath()).filter(p -> p.getFileName()
                    .startsWith(className + ".class")).findFirst();
            if (clazzFile.isPresent()) {
                Class<FastSerializer<?>> fastSerializerClass = (Class<FastSerializer<?>>) classLoader
                        .loadClass(FastSerializerGeneratorBase.GENERATED_PACKAGE_NAME + "." + className);

                return fastSerializerClass.newInstance();
            } else {
                FastSpecificSerializerGenerator<?> generator = new FastSpecificSerializerGenerator<>(
                        schema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));

                return generator.generateSerializer();
            }
        } catch (FastDeserializerGeneratorException e) {
            LOGGER.log(Level.WARNING, "serializer generation exception", e);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "serializer class instantiation exception", e);
        }

        return (d, e) -> {
            new SpecificDatumWriter<>(schema).write(d, e);
        };
    }

    private FastSerializer<?> buildGenericSerializer(Schema schema) {
        try {
            String className = FastSerializerGeneratorBase.getClassName(schema, "Generic");
            Optional<Path> clazzFile = Files.walk(classesDir.toPath()).filter(p -> p.getFileName()
                    .startsWith(className + ".class")).findFirst();
            if (clazzFile.isPresent()) {
                Class<FastSerializer<?>> fastSerializerClass = (Class<FastSerializer<?>>) classLoader
                        .loadClass(FastSerializerGeneratorBase.GENERATED_PACKAGE_NAME + "." + className);

                return fastSerializerClass.getConstructor(Schema.class).newInstance(schema);
            } else {
                FastGenericSerializerGenerator<?> generator = new FastGenericSerializerGenerator<>(
                        schema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));

                return generator.generateSerializer();
            }

        } catch (FastDeserializerGeneratorException e) {
            LOGGER.log(Level.WARNING, "serializer generation exception", e);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "serializer class instantiation exception", e);
        }

        return (d, e) -> {
            new GenericDatumWriter<>(schema).write(d, e);
        };
    }

    private Executor getDefaultExecutor() {
        return Executors.newFixedThreadPool(2, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable runnable) {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                thread.setName("avro-fastserde-compile-thread-" + threadNumber.getAndIncrement());
                return thread;
            }
        });
    }
}
