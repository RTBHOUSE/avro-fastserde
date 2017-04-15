package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.io.parsing.Symbol;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;

public abstract class FastDeserializerGeneratorBase<T> {

    public static final String GENERATED_PACKAGE_NAME = "com.rtbhouse.utils.avro.deserialization.generated";
    public static final String GENERATED_SOURCES_PATH = "/com/rtbhouse/utils/avro/deserialization/generated/";

    protected JCodeModel codeModel;
    protected JDefinedClass deserializerClass;
    protected final Schema writer;
    protected final Schema reader;
    private File destination;
    private ClassLoader classLoader;
    private String compileClassPath;

    FastDeserializerGeneratorBase(Schema writer, Schema reader, File destination, ClassLoader classLoader,
            String compileClassPath) {
        this.writer = writer;
        this.reader = reader;
        this.destination = destination;
        this.classLoader = classLoader;
        this.compileClassPath = compileClassPath;
        codeModel = new JCodeModel();
    }

    public abstract FastDeserializer<T> generateDeserializer();

    @SuppressWarnings("unchecked")
    protected Class<FastDeserializer<T>> compileClass(final String className) throws IOException,
            ClassNotFoundException {
        codeModel.build(destination);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        int compileResult;
        if (compileClassPath != null) {
            compileResult = compiler.run(null, null, null,
                    "-cp", compileClassPath,
                    destination.getAbsolutePath() + GENERATED_SOURCES_PATH + className + ".java"
                    );
        } else {
            compileResult = compiler.run(null, null, null, destination.getAbsolutePath()
                    + GENERATED_SOURCES_PATH
                    + className + ".java");
        }

        if (compileResult != 0) {
            throw new FastDeserializerGeneratorException("unable to compile:" + className);
        }

        return (Class<FastDeserializer<T>>) classLoader.loadClass(GENERATED_PACKAGE_NAME + "."
                + className);
    }

    protected ListIterator<Symbol> actionIterator(FieldAction action) {
        ListIterator<Symbol> actionIterator = action.getSymbolIterator() != null ? action
                .getSymbolIterator() : Lists.newArrayList(reverseSymbolArray(action.getSymbol().production))
                .listIterator();

        while (actionIterator.hasNext()) {
            Symbol symbol = actionIterator.next();

            if (symbol instanceof Symbol.ErrorAction) {
                throw new FastDeserializerGeneratorException(((Symbol.ErrorAction) symbol).msg);
            }

            if (symbol instanceof Symbol.FieldOrderAction) {
                break;
            }
        }

        return actionIterator;
    }

    protected FieldAction seekFieldAction(boolean shouldReadCurrent, Schema.Field field,
            ListIterator<Symbol> symbolIterator) {

        Schema.Type type = field.schema().getType();

        if (!shouldReadCurrent) {
            return FieldAction.fromValues(type, false, EMPTY_SYMBOL);
        }

        boolean shouldRead = true;
        Symbol fieldSymbol = END_SYMBOL;

        if (Schema.Type.RECORD.equals(type)) {
            return FieldAction.fromValues(type, true, symbolIterator);
        }

        while (symbolIterator.hasNext()) {
            Symbol symbol = symbolIterator.next();

            if (symbol instanceof Symbol.ErrorAction) {
                throw new FastDeserializerGeneratorException(((Symbol.ErrorAction) symbol).msg);
            }

            if (symbol instanceof Symbol.SkipAction) {
                shouldRead = false;
                fieldSymbol = symbol;
                break;
            }

            if (symbol instanceof Symbol.WriterUnionAction) {
                if (symbolIterator.hasNext()) {
                    symbol = symbolIterator.next();

                    if (symbol instanceof Symbol.Alternative) {
                        shouldRead = true;
                        fieldSymbol = symbol;
                        break;
                    }
                }
            }

            if (symbol.kind == Symbol.Kind.TERMINAL) {
                shouldRead = true;
                if (symbolIterator.hasNext()) {
                    symbol = symbolIterator.next();

                    if (symbol instanceof Symbol.Repeater) {
                        fieldSymbol = symbol;
                    } else {
                        fieldSymbol = symbolIterator.previous();
                    }
                } else if (!symbolIterator.hasNext() && getSymbolPrintName(symbol) != null) {
                    fieldSymbol = symbol;
                }
                break;
            }
        }

        return FieldAction.fromValues(type, shouldRead, fieldSymbol);
    }

    protected static final class FieldAction {

        private Schema.Type type;
        private boolean shouldRead;
        private Symbol symbol;
        private ListIterator<Symbol> symbolIterator;

        private FieldAction(Schema.Type type, boolean shouldRead, Symbol symbol) {
            this.type = type;
            this.shouldRead = shouldRead;
            this.symbol = symbol;
        }

        private FieldAction(Schema.Type type, boolean shouldRead, ListIterator<Symbol> symbolIterator) {
            this.type = type;
            this.shouldRead = shouldRead;
            this.symbolIterator = symbolIterator;
        }

        public Schema.Type getType() {
            return type;
        }

        public boolean getShouldRead() {
            return shouldRead;
        }

        public Symbol getSymbol() {
            return symbol;
        }

        public ListIterator<Symbol> getSymbolIterator() {
            return symbolIterator;
        }

        public static FieldAction fromValues(Schema.Type type, boolean read, Symbol symbol) {
            return new FieldAction(type, read, symbol);
        }

        public static FieldAction fromValues(Schema.Type type, boolean read, ListIterator<Symbol> symbolIterator) {
            return new FieldAction(type, read, symbolIterator);
        }
    }

    protected static final Symbol EMPTY_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[] {}) {
    };

    protected static final Symbol END_SYMBOL = new Symbol(Symbol.Kind.TERMINAL, new Symbol[] {}) {
    };

    protected static Symbol[] reverseSymbolArray(Symbol[] symbols) {
        Symbol[] reversedSymbols = new Symbol[symbols.length];

        for (int i = 0; i < symbols.length; i++) {
            reversedSymbols[symbols.length - i - 1] = symbols[i];
        }

        return reversedSymbols;
    }

    public static String getClassName(Schema writerSchema, Schema readerSchema, String description) {
        Integer writerSchemaId = Math.abs(getSchemaId(writerSchema));
        Integer readerSchemaId = Math.abs(getSchemaId(readerSchema));
        if (Schema.Type.RECORD.equals(readerSchema.getType())) {
            return readerSchema.getName() + description + "Deserializer"
                    + writerSchemaId + "_" + readerSchemaId;
        } else if (Schema.Type.ARRAY.equals(readerSchema.getType())) {
            return "Array" + description + "Deserializer"
                    + writerSchemaId + "_" + readerSchemaId;
        } else if (Schema.Type.MAP.equals(readerSchema.getType())) {
            return "Map" + description + "Deserializer"
                    + writerSchemaId + "_" + readerSchemaId;
        }
        throw new FastDeserializerGeneratorException("Unsupported return type: " + readerSchema.getType());
    }

    protected static String getVariableName(String name) {
        return name + nextRandomInt();
    }

    protected static String getSymbolPrintName(Symbol symbol) {
        String printName;
        try {
            Field field = symbol.getClass().getDeclaredField("printName");

            field.setAccessible(true);
            printName = (String) field.get(symbol);
            field.setAccessible(false);

        } catch (ReflectiveOperationException e) {
            throw new FastDeserializerGeneratorException(e);
        }

        return printName;
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
