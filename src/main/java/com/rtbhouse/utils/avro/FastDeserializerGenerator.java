package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.sun.codemodel.*;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonNode;

public class FastDeserializerGenerator<T> extends FastDeserializerGeneratorBase<T> {

    private static final String DECODER = "decoder";

    private boolean useGenericTypes;
    private JMethod schemaMapMethod;
    private JFieldVar schemaMapField;
    private Map<Integer, Schema> schemaMap = new HashMap<>();
    private Map<String, JMethod> deserializeMethodMap = new HashMap<>();
    private Map<String, JMethod> skipMethodMap = new HashMap<>();
    private SchemaMapper schemaMapper;

    FastDeserializerGenerator(boolean useGenericTypes, Schema writer, Schema reader, File destination,
            ClassLoader classLoader,
            String compileClassPath) {
        super(writer, reader, destination, classLoader, compileClassPath);
        this.useGenericTypes = useGenericTypes;
        this.schemaMapper = new SchemaMapper(codeModel, useGenericTypes);
    }

    public FastDeserializer<T> generateDeserializer() {
        String className = getClassName(writer, reader, useGenericTypes ? "Generic" : "Specific");
        JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

        try {
            deserializerClass = classPackage._class(className);

            JVar readerSchemaVar = deserializerClass.field(JMod.PRIVATE | JMod.FINAL, Schema.class, "readerSchema");
            JMethod constructor = deserializerClass.constructor(JMod.PUBLIC);
            JVar constructorParam = constructor.param(Schema.class, "readerSchema");
            constructor.body().assign(JExpr.refthis(readerSchemaVar.name()), constructorParam);

            Schema aliasedWriterSchema = Schema.applyAliases(writer, reader);
            Symbol resolvingGrammar = new ResolvingGrammarGenerator().generate(aliasedWriterSchema, reader);
            FieldAction fieldAction = FieldAction.fromValues(aliasedWriterSchema.getType(), true, resolvingGrammar);

            if (useGenericTypes) {
                schemaMapField = deserializerClass.field(JMod.PRIVATE,
                        codeModel.ref(Map.class).narrow(Integer.class).narrow(Schema.class), "readerSchemaMap");
                schemaMapMethod = deserializerClass.method(JMod.PRIVATE | JMod.FINAL, void.class, "schemaMap");
                constructor.body().invoke(schemaMapMethod);
                schemaMapMethod.body().assign(schemaMapField,
                        JExpr._new(codeModel.ref(HashMap.class).narrow(Integer.class).narrow(Schema.class)));
                registerSchema(aliasedWriterSchema, readerSchemaVar);
            }

            JClass readerSchemaClass = schemaMapper.classFromSchema(reader);
            JClass writerSchemaClass = schemaMapper.classFromSchema(aliasedWriterSchema);

            deserializerClass._implements(codeModel.ref(FastDeserializer.class).narrow(writerSchemaClass));
            JMethod deserializeMethod = deserializerClass.method(JMod.PUBLIC, readerSchemaClass, "deserialize");

            JVar result = declareValueVar("result", aliasedWriterSchema, deserializeMethod.body());

            JTryBlock tryDeserializeBlock = deserializeMethod.body()._try();

            switch (aliasedWriterSchema.getType()) {
            case RECORD:
                processRecord(readerSchemaVar, result, aliasedWriterSchema, reader, tryDeserializeBlock.body(),
                        fieldAction);
                break;
            case ARRAY:
                processArray(readerSchemaVar, result, null, aliasedWriterSchema, reader, tryDeserializeBlock.body(),
                        fieldAction);
                break;
            case MAP:
                processMap(readerSchemaVar, result, null, aliasedWriterSchema, reader, tryDeserializeBlock.body(),
                        fieldAction);
                break;
            default:
                throw new FastDeserializerGeneratorException(
                        "Incorrect top-level writer schema: " + aliasedWriterSchema.getType());
            }

            JCatchBlock catchBlock = tryDeserializeBlock._catch(codeModel.ref(Throwable.class));
            JVar exceptionVar = catchBlock.param("e");
            catchBlock.body()._throw(JExpr._new(codeModel.ref(IOException.class)).arg(exceptionVar));

            deserializeMethod._throws(codeModel.ref(IOException.class));
            deserializeMethod.param(Decoder.class, DECODER);

            deserializeMethod.body()._return(result);

            Class<FastDeserializer<T>> clazz = compileClass(className);
            return clazz.getConstructor(Schema.class).newInstance(reader);
        } catch (JClassAlreadyExistsException e) {
            throw new FastDeserializerGeneratorException("Class: " + className + " already exists");
        } catch (Exception e) {
            throw new FastDeserializerGeneratorException(e);
        }
    }

    private void processRecord(JVar recordSchemaVar, JVar recordVar,
            final Schema recordWriterSchema, final Schema recordReaderSchema, JBlock body, FieldAction recordAction) {

        ListIterator<Symbol> actionIterator = actionIterator(recordAction);

        if (methodAlreadyDefined(recordWriterSchema, recordAction.getShouldRead())) {
            JExpression readingExpression = JExpr.invoke(getMethod(recordWriterSchema, recordAction.getShouldRead()))
                    .arg(JExpr.direct(DECODER));
            if (recordVar != null) {
                body.assign(recordVar, readingExpression);
            } else {
                body.add((JStatement) readingExpression);
            }

            // seek through actionIterator
            for (Schema.Field field : recordWriterSchema.getFields()) {
                FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
                if (action.getSymbol() == END_SYMBOL) {
                    break;
                }
            }
            if (!recordAction.getShouldRead()) {
                return;
            }
            // seek through actionIterator also for default values
            Set<String> fieldNamesSet = recordWriterSchema.getFields().stream().map(Schema.Field::name)
                    .collect(Collectors.toSet());
            for (Schema.Field readerField : recordReaderSchema.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                }
            }
            return;
        }

        JMethod method = createMethod(recordWriterSchema, recordAction.getShouldRead());
        method._throws(Throwable.class);

        if (recordVar != null) {
            body.assign(recordVar, JExpr.invoke(method).arg(JExpr.direct(DECODER)));
        } else {
            body.invoke(method).arg(JExpr.direct(DECODER));
        }

        body = method.body();

        JVar result = null;
        if (recordAction.getShouldRead()) {
            JClass recordClass = schemaMapper.classFromSchema(recordWriterSchema);
            JInvocation newRecord = JExpr._new(schemaMapper.classFromSchema(recordWriterSchema, false));
            if (useGenericTypes) {
                newRecord = newRecord.arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(recordWriterSchema))));
            }
            result = body.decl(recordClass, "result", newRecord);
        }

        for (Schema.Field field : recordWriterSchema.getFields()) {

            FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
            if (action.getSymbol() == END_SYMBOL) {
                break;
            }

            Schema.Field readerField = null;
            Schema readerFieldSchema = null;
            if (action.getShouldRead()) {
                readerField = recordReaderSchema.getField(field.name());
                readerFieldSchema = readerField.schema();
            }

            JExpression fieldValueExpression;
            if (SchemaMapper.isComplexType(field.schema())) {
                JVar fieldValueVar = null;
                JVar fieldSchemaVar = null;
                if (action.getShouldRead()) {
                    fieldValueVar = body.decl(schemaMapper.classFromSchema(field.schema()),
                            getVariableName(field.name()), JExpr._null());
                    if (useGenericTypes)
                        fieldSchemaVar = declareSchemaVar(field.schema(), field.name(),
                                recordSchemaVar.invoke("getField").arg(field.name()).invoke("schema"));
                }
                switch (field.schema().getType()) {
                case RECORD:
                    processRecord(fieldSchemaVar, fieldValueVar, field.schema(), readerFieldSchema, body, action);
                    break;
                case ARRAY:
                    processArray(fieldSchemaVar, fieldValueVar, field.name(), field.schema(), readerFieldSchema, body,
                            action);
                    break;
                case MAP:
                    processMap(fieldSchemaVar, fieldValueVar, field.name(), field.schema(), readerFieldSchema, body,
                            action);
                    break;
                case UNION:
                    processUnion(fieldSchemaVar, fieldValueVar, field.name(), field.schema(), readerFieldSchema, body,
                            action);
                    break;
                default:
                    throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
                }
                fieldValueExpression = fieldValueVar;
            } else {
                switch (field.schema().getType()) {
                case ENUM:
                    fieldValueExpression = processEnum(field.schema(), body, action);
                    break;
                case FIXED:
                    fieldValueExpression = processFixed(field.schema(), body, action);
                    break;
                default:
                    fieldValueExpression = processPrimitive(field.schema(), body, action);
                }

            }
            if (action.getShouldRead()) {
                if (fieldValueExpression == null) {
                    throw new FastDeserializerGeneratorException("Non-null value expression was expected!");
                }
                body.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(fieldValueExpression);
            }
        }

        // Handle default values
        if (recordAction.getShouldRead()) {
            Set<String> fieldNamesSet = recordWriterSchema.getFields().stream().map(Schema.Field::name)
                    .collect(Collectors.toSet());
            for (Schema.Field readerField : recordReaderSchema.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                    JVar schemaVar = null;
                    if (useGenericTypes) {
                        schemaVar = declareSchemaVariableForRecordField(readerField.name(), readerField.schema(),
                                recordSchemaVar);
                    }
                    JExpression value = parseDefaultValue(readerField.schema(), readerField.defaultValue(), body,
                            schemaVar, readerField.name());
                    body.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(value);
                }
            }
        }

        if (recordAction.getShouldRead()) {
            body._return(result);
        }
    }

    private JExpression parseDefaultValue(Schema schema, JsonNode defaultValue, JBlock body, JVar schemaVar,
            String fieldName) {
        Schema.Type schemaType = schema.getType();
        // The default value of union is of the first defined type
        if (Schema.Type.UNION.equals(schemaType)) {
            schema = schema.getTypes().get(0);
            schemaType = schema.getType();
            if (useGenericTypes) {
                JInvocation optionSchemaExpression = schemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(0));
                schemaVar = declareSchemaVar(schema, fieldName, optionSchemaExpression);
            }
        }
        // And default value of null is always null
        if (Schema.Type.NULL.equals(schemaType)) {
            return JExpr._null();
        }

        if (SchemaMapper.isComplexType(schema)) {
            JClass defaultValueClass = schemaMapper.classFromSchema(schema, false);
            JInvocation valueInitializationExpr = JExpr._new(defaultValueClass);

            JVar valueVar;
            switch (schemaType) {
            case RECORD:
                if (useGenericTypes) {
                    valueInitializationExpr = valueInitializationExpr.arg(getSchemaExpr(schema));
                }
                valueVar = body.decl(defaultValueClass, getVariableName("default" + schema.getName()),
                        valueInitializationExpr);
                for (Iterator<Map.Entry<String, JsonNode>> it = defaultValue.getFields(); it.hasNext();) {
                    Map.Entry<String, JsonNode> subFieldEntry = it.next();
                    Schema.Field subField = schema.getField(subFieldEntry.getKey());

                    JVar fieldSchemaVar = null;
                    if (useGenericTypes) {
                        fieldSchemaVar = declareSchemaVariableForRecordField(subField.name(), subField.schema(),
                                schemaVar);
                    }
                    JExpression fieldValue = parseDefaultValue(subField.schema(), subFieldEntry.getValue(), body,
                            fieldSchemaVar, subField.name());
                    body.invoke(valueVar, "put").arg(JExpr.lit(subField.pos())).arg(fieldValue);
                }
                break;
            case ARRAY:
                JVar elementSchemaVar = null;
                if (useGenericTypes) {
                    valueInitializationExpr = valueInitializationExpr
                            .arg(JExpr.lit(defaultValue.size())).arg(getSchemaExpr(schema));
                    elementSchemaVar = declareSchemaVar(schema.getElementType(), "defaultElementSchema",
                            schemaVar.invoke("getElementType"));
                }

                valueVar = body.decl(defaultValueClass, getVariableName("defaultArray"), valueInitializationExpr);

                for (JsonNode arrayElementValue : defaultValue) {
                    JExpression arrayElementExpression = parseDefaultValue(schema.getElementType(), arrayElementValue,
                            body, elementSchemaVar, "arrayValue");
                    body.invoke(valueVar, "add").arg(arrayElementExpression);
                }
                break;
            case MAP:
                JVar mapValueSchemaVar = null;
                if (useGenericTypes) {
                    mapValueSchemaVar = declareSchemaVar(schema.getValueType(), "defaultMapValueSchema",
                            schemaVar.invoke("getValueType"));
                }

                valueVar = body.decl(defaultValueClass, getVariableName("defaultMap"), valueInitializationExpr);

                for (Iterator<Map.Entry<String, JsonNode>> it = defaultValue.getFields(); it.hasNext();) {
                    Map.Entry<String, JsonNode> mapEntry = it.next();
                    JExpression mapKeyExpr;
                    if (SchemaMapper.haveStringableKey(schema)) {
                        mapKeyExpr = JExpr._new(schemaMapper.keyClassFromMapSchema(schema)).arg(mapEntry.getKey());
                    } else {
                        mapKeyExpr = JExpr.lit(mapEntry.getKey());
                    }
                    JExpression mapEntryValueExpression = parseDefaultValue(schema.getValueType(), mapEntry.getValue(),
                            body,
                            mapValueSchemaVar, "mapElement");
                    body.invoke(valueVar, "put").arg(mapKeyExpr).arg(mapEntryValueExpression);
                }
                break;
            default:
                throw new FastDeserializerGeneratorException("Incorrect schema type in default value!");
            }
            return valueVar;
        } else {
            switch (schemaType) {
            case ENUM:
                return schemaMapper.getEnumValueByName(schema, JExpr.lit(defaultValue.getTextValue()),
                        getSchemaExpr(schema));
            case FIXED:
                JArray fixedBytesArray = JExpr.newArray(codeModel.BYTE);
                for (char b : defaultValue.getTextValue().toCharArray()) {
                    fixedBytesArray.add(JExpr.lit((byte) b));
                }
                return schemaMapper.getFixedValue(schema, fixedBytesArray, getSchemaExpr(schema));
            case BYTES:
                JArray bytesArray = JExpr.newArray(codeModel.BYTE);
                for (byte b : defaultValue.getTextValue().getBytes()) {
                    bytesArray.add(JExpr.lit(b));
                }
                return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);
            case STRING:
                return schemaMapper.getStringableValue(schema, JExpr.lit(defaultValue.getTextValue()));
            case INT:
                return JExpr.lit(defaultValue.getIntValue());
            case LONG:
                return JExpr.lit(defaultValue.getLongValue());
            case FLOAT:
                return JExpr.lit((float) defaultValue.getDoubleValue());
            case DOUBLE:
                return JExpr.lit(defaultValue.getDoubleValue());
            case BOOLEAN:
                return JExpr.lit(defaultValue.getBooleanValue());
            case NULL:
            default:
                throw new FastDeserializerGeneratorException("Incorrect schema type in default value!");
            }
        }
    }

    private void processUnion(JVar unionSchemaVar, JVar unionVar, final String name,
            final Schema unionSchema, final Schema readerUnionSchema, JBlock body, FieldAction action) {
        JVar unionIndex = body.decl(codeModel.INT, getVariableName("unionIndex"),
                JExpr.direct(DECODER + ".readIndex()"));
        JClass unionClass = schemaMapper.classFromSchema(unionSchema);

        JConditional ifBlock = null;

        for (int i = 0; i < unionSchema.getTypes().size(); i++) {
            Schema optionSchema = unionSchema.getTypes().get(i);
            Schema readerOptionSchema = null;
            FieldAction unionAction;

            if (Schema.Type.NULL.equals(optionSchema.getType())) {
                body._if(unionIndex.eq(JExpr.lit(i)))._then().directStatement(DECODER + ".readNull();");
                continue;
            }

            if (action.getShouldRead()) {
                readerOptionSchema = readerUnionSchema.getTypes().get(i);
                Symbol.Alternative alternative = null;
                if (action.getSymbol() instanceof Symbol.Alternative) {
                    alternative = (Symbol.Alternative) action.getSymbol();
                } else if (action.getSymbol().production != null) {
                    for (Symbol symbol : action.getSymbol().production) {
                        if (symbol instanceof Symbol.Alternative) {
                            alternative = (Symbol.Alternative) symbol;
                            break;
                        }
                    }
                }

                if (alternative == null) {
                    throw new FastDeserializerGeneratorException("Unable to determine action for field: " + name);
                }

                Symbol.UnionAdjustAction unionAdjustAction = (Symbol.UnionAdjustAction) alternative.symbols[i].production[0];
                unionAction = FieldAction.fromValues(optionSchema.getType(), action.getShouldRead(),
                        unionAdjustAction.symToParse);
            } else {
                unionAction = FieldAction.fromValues(optionSchema.getType(), false, EMPTY_SYMBOL);
            }

            JExpression condition = unionIndex.eq(JExpr.lit(i));
            ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
            JBlock thenBlock = ifBlock._then();

            JVar optionSchemaVar = null;
            if (useGenericTypes && unionAction.getShouldRead()) {
                JInvocation optionSchemaExpression = unionSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(i));
                optionSchemaVar = declareSchemaVar(optionSchema, name, optionSchemaExpression);
            }

            JExpression optionValue;
            if (SchemaMapper.isComplexType(optionSchema)) {

                JVar optionValueVar = null;

                if (unionAction.getShouldRead()) {
                    optionValueVar = declareValueVar(name, optionSchema, thenBlock);
                }
                switch (unionAction.getType()) {
                case RECORD:
                    processRecord(optionSchemaVar, optionValueVar, optionSchema, readerOptionSchema, thenBlock,
                            unionAction);
                    break;
                case ARRAY:
                    processArray(optionSchemaVar, optionValueVar, name, optionSchema, readerOptionSchema, thenBlock,
                            unionAction);
                    break;
                case MAP:
                    processMap(optionSchemaVar, optionValueVar, name, optionSchema, readerOptionSchema, thenBlock,
                            unionAction);
                    break;
                default:
                    throw new FastDeserializerGeneratorException("Unexpected complex type: " + action.getType());
                }
                optionValue = optionValueVar;
            } else {
                switch (unionAction.getType()) {
                case ENUM:
                    optionValue = processEnum(optionSchema, thenBlock, unionAction);
                    break;
                case FIXED:
                    optionValue = processFixed(optionSchema, thenBlock, unionAction);
                    break;
                default:
                    optionValue = processPrimitive(optionSchema, thenBlock, unionAction);
                }
            }
            if (unionAction.getShouldRead()) {
                if (optionValue == null) {
                    throw new FastDeserializerGeneratorException(
                            "Value expression was expected, got null instead!");
                }
                thenBlock.assign(unionVar, JExpr.cast(unionClass, optionValue));
            }
        }
    }

    private void processArray(JVar arraySchemaVar, JVar arrayVar, final String name,
            final Schema arraySchema, final Schema readerArraySchema, JBlock body, FieldAction action) {

        if (action.getShouldRead()) {
            Symbol valuesActionSymbol = null;
            for (Symbol symbol : action.getSymbol().production) {
                if (Symbol.Kind.REPEATER.equals(symbol.kind)
                        && "array-end".equals(getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
                    valuesActionSymbol = symbol;
                    break;
                }
            }

            if (valuesActionSymbol == null) {
                throw new FastDeserializerGeneratorException("Unable to determine action for array: " + name);
            }

            action = FieldAction.fromValues(arraySchema.getElementType().getType(), action.getShouldRead(),
                    valuesActionSymbol);
        } else {
            action = FieldAction.fromValues(arraySchema.getElementType().getType(), false, EMPTY_SYMBOL);
        }

        JVar chunkLen = body.decl(codeModel.LONG, getVariableName("chunkLen"),
                JExpr.direct(DECODER + ".readArrayStart()"));

        JConditional conditional = body._if(chunkLen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        JClass arrayClass = schemaMapper.classFromSchema(arraySchema, false);

        if (action.getShouldRead()) {
            JInvocation newArrayExp = JExpr._new(arrayClass);
            if (useGenericTypes) {
                newArrayExp = newArrayExp.arg(JExpr.cast(codeModel.INT, chunkLen)).arg(getSchemaExpr(arraySchema));
            }
            ifBlock.assign(arrayVar, newArrayExp);
            JBlock elseBlock = conditional._else();
            if (useGenericTypes) {
                elseBlock.assign(arrayVar, JExpr._new(arrayClass).arg(JExpr.lit(0)).arg(getSchemaExpr(arraySchema)));
            } else {
                elseBlock.assign(arrayVar, codeModel.ref(Collections.class).staticInvoke("emptyList"));
            }
        }

        JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunkLen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        Schema readerArrayElementSchema = null;
        JExpression elementValueExpression;
        JVar elementSchemaVar = null;
        if (useGenericTypes) {
            elementSchemaVar = declareSchemaVar(arraySchema.getElementType(), name + "ArraySchema",
                    arraySchemaVar.invoke("getElementType"));
        }

        if (SchemaMapper.isComplexType(arraySchema.getElementType())) {
            JVar elementValueVar = null;
            if (action.getShouldRead()) {
                elementValueVar = declareValueVar(name, arraySchema.getElementType(), forBody);
                readerArrayElementSchema = readerArraySchema.getElementType();
            }
            String elemName = name + "Elem";

            switch (arraySchema.getElementType().getType()) {
            case RECORD:
                processRecord(elementSchemaVar, elementValueVar, arraySchema.getElementType(), readerArrayElementSchema,
                        forBody, action);
                break;
            case ARRAY:
                processArray(elementSchemaVar, elementValueVar, elemName, arraySchema.getElementType(),
                        readerArrayElementSchema, forBody, action);
                break;
            case MAP:
                processMap(elementSchemaVar, elementValueVar, elemName, arraySchema.getElementType(),
                        readerArrayElementSchema, forBody, action);
                break;
            case UNION:
                processUnion(elementSchemaVar, elementValueVar, elemName, arraySchema.getElementType(),
                        readerArrayElementSchema, forBody, action);
                break;
            }
            elementValueExpression = elementValueVar;
        } else {
            switch (arraySchema.getElementType().getType()) {
            case ENUM:
                elementValueExpression = processEnum(arraySchema.getElementType(), forBody, action);
                break;
            case FIXED:
                elementValueExpression = processFixed(arraySchema.getElementType(), forBody, action);
                break;
            default:
                elementValueExpression = processPrimitive(arraySchema.getElementType(), forBody, action);
            }
        }
        if (action.getShouldRead()) {
            if (elementValueExpression == null) {
                throw new FastDeserializerGeneratorException("Value expression was expected, got null instead!");
            }
            forBody.invoke(arrayVar, "add").arg(elementValueExpression);
        }
        doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".arrayNext()"));
    }

    private void processMap(JVar mapSchemaVar, JVar mapVar, final String name,
            final Schema mapSchema, final Schema readerMapSchema, JBlock body, FieldAction action) {

        if (action.getShouldRead()) {
            Symbol valuesActionSymbol = null;
            for (Symbol symbol : action.getSymbol().production) {
                if (Symbol.Kind.REPEATER.equals(symbol.kind)
                        && "map-end".equals(getSymbolPrintName(((Symbol.Repeater) symbol).end))) {
                    valuesActionSymbol = symbol;
                    break;
                }
            }

            if (valuesActionSymbol == null) {
                throw new FastDeserializerGeneratorException("unable to determine action for map: " + name);
            }

            action = FieldAction.fromValues(mapSchema.getValueType().getType(), action.getShouldRead(),
                    valuesActionSymbol);
        } else {
            action = FieldAction.fromValues(mapSchema.getValueType().getType(), false, EMPTY_SYMBOL);
        }

        JVar chunkLen = body.decl(codeModel.LONG, getVariableName("chunkLen"),
                JExpr.direct(DECODER + ".readMapStart()"));

        JConditional conditional = body._if(chunkLen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        if (action.getShouldRead()) {
            ifBlock.assign(mapVar, JExpr._new(schemaMapper.classFromSchema(mapSchema, false)));
            JBlock elseBlock = conditional._else();
            elseBlock.assign(mapVar, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
        }

        JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunkLen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        JClass keyClass = schemaMapper.keyClassFromMapSchema(mapSchema);
        JExpression keyValueExpression = JExpr.direct(DECODER + ".readString()");
        if (SchemaMapper.haveStringableKey(mapSchema)) {
            keyValueExpression = JExpr._new(keyClass).arg(keyValueExpression);
        }

        JVar key = forBody.decl(keyClass, getVariableName("key"), keyValueExpression);
        JExpression mapValueExpression;
        JVar mapValueSchemaVar = null;
        if (useGenericTypes)
            mapValueSchemaVar = declareSchemaVar(mapSchema.getValueType(), getVariableName(name + "MapValue"),
                    mapSchemaVar.invoke("getValueType"));

        if (SchemaMapper.isComplexType(mapSchema.getValueType())) {
            JVar mapValueVar = null;
            Schema readerMapValueSchema = readerMapSchema.getValueType();

            if (action.getShouldRead()) {
                mapValueVar = declareValueVar(name, mapSchema.getValueType(), forBody);
            }
            switch (mapSchema.getValueType().getType()) {
            case RECORD:
                processRecord(mapValueSchemaVar, mapValueVar, mapSchema.getValueType(), readerMapValueSchema, forBody,
                        action);
                break;
            case ARRAY:
                processArray(mapValueSchemaVar, mapValueVar, name, mapSchema.getValueType(), readerMapValueSchema,
                        forBody, action);
                break;
            case MAP:
                processMap(mapValueSchemaVar, mapValueVar, name, mapSchema.getValueType(), readerMapValueSchema,
                        forBody, action);
                break;
            case UNION:
                processUnion(mapValueSchemaVar, mapValueVar, name, mapSchema.getValueType(), readerMapValueSchema,
                        forBody, action);
                break;
            default:
                throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
            }
            mapValueExpression = mapValueVar;
        } else {
            switch (mapSchema.getValueType().getType()) {
            case ENUM:
                mapValueExpression = processEnum(mapSchema.getValueType(), forBody, action);
                break;
            case FIXED:
                mapValueExpression = processFixed(mapSchema.getValueType(), forBody, action);
                break;
            default:
                mapValueExpression = processPrimitive(mapSchema.getValueType(), forBody, action);
            }
        }
        doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".mapNext()"));

        if (action.getShouldRead()) {
            if (mapValueExpression == null) {
                throw new FastDeserializerGeneratorException(
                        "Value expression was expected, got null instead!");
            }
            forBody.invoke(mapVar, "put").arg(key).arg(mapValueExpression);
        }
    }

    private JExpression processFixed(final Schema schema, JBlock body, FieldAction action) {
        if (action.getShouldRead()) {
            JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getVariableName(schema.getName()))
                    .init(JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));

            body.directStatement(DECODER + ".readFixed(" + fixedBuffer.name() + ");");

            JInvocation createFixed = JExpr._new(schemaMapper.classFromSchema(schema));
            if (useGenericTypes)
                createFixed = createFixed.arg(getSchemaExpr(schema));

            return createFixed.arg(fixedBuffer);
        } else {
            body.directStatement(DECODER + ".skipFixed(" + schema.getFixedSize() + ");");
            return null;
        }
    }

    private JExpression processEnum(final Schema schema, final JBlock body, FieldAction action) {

        if (action.getShouldRead()) {

            Symbol.EnumAdjustAction enumAdjustAction = null;
            if (action.getSymbol() instanceof Symbol.EnumAdjustAction) {
                enumAdjustAction = (Symbol.EnumAdjustAction) action.getSymbol();
            } else {
                for (Symbol symbol : action.getSymbol().production) {
                    if (symbol instanceof Symbol.EnumAdjustAction) {
                        enumAdjustAction = (Symbol.EnumAdjustAction) symbol;
                    }
                }
            }

            boolean enumOrderCorrect = true;
            for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
                Object adjustment = enumAdjustAction.adjustments[i];
                if (adjustment instanceof String) {
                    throw new FastDeserializerGeneratorException(
                            schema.getName() + " enum label impossible to deserialize: " + adjustment.toString());
                } else if (!adjustment.equals(i)) {
                    enumOrderCorrect = false;
                }
            }

            JExpression newEnum;
            JExpression enumValueExpr = JExpr.direct(DECODER + ".readEnum()");

            if (enumOrderCorrect) {
                newEnum = schemaMapper.getEnumValueByIndex(schema, enumValueExpr, getSchemaExpr(schema));
            } else {
                JVar enumIndex = body.decl(codeModel.INT, getVariableName("enumIndex"), enumValueExpr);
                newEnum = body.decl(schemaMapper.classFromSchema(schema), getVariableName("enumValue"), JExpr._null());

                for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
                    JExpression ithVal = schemaMapper
                            .getEnumValueByIndex(schema, JExpr.lit((Integer) enumAdjustAction.adjustments[i]),
                                    getSchemaExpr(schema));
                    body._if(enumIndex.eq(JExpr.lit(i)))._then().assign((JVar) newEnum, ithVal);
                }
            }

            return newEnum;
        } else {
            body.directStatement(DECODER + ".readEnum();");
            return null;
        }

    }

    private JExpression processPrimitive(final Schema schema, JBlock body, FieldAction action) {

        String readFunction;
        switch (schema.getType()) {
        case STRING:
            readFunction = action.getShouldRead() ? "readString()" : "skipString()";
            break;
        case BYTES:
            readFunction = "readBytes(null)";
            break;
        case INT:
            readFunction = "readInt()";
            break;
        case LONG:
            readFunction = "readLong()";
            break;
        case FLOAT:
            readFunction = "readFloat()";
            break;
        case DOUBLE:
            readFunction = "readDouble()";
            break;
        case BOOLEAN:
            readFunction = "readBoolean()";
            break;
        default:
            throw new FastDeserializerGeneratorException(
                    "Unsupported primitive schema of type: " + schema.getType());
        }

        JExpression primitiveValueExpression = JExpr.direct("decoder." + readFunction);
        if (action.getShouldRead()) {
            if (schema.getType().equals(Schema.Type.STRING) && SchemaMapper.isStringable(schema)) {
                primitiveValueExpression = JExpr._new(schemaMapper.classFromSchema(schema))
                        .arg(primitiveValueExpression);
            }

            return primitiveValueExpression;
        } else {
            body.directStatement(DECODER + "." + readFunction + ";");
            return JExpr._null();
        }
    }

    private JVar declareValueVar(final String name, final Schema schema, JBlock block) {
        if (SchemaMapper.isComplexType(schema)) {
            return block.decl(schemaMapper.classFromSchema(schema), getVariableName(name), JExpr._null());
        } else {
            throw new FastDeserializerGeneratorException("Only complex types allowed!");
        }
    }

    private JVar declareSchemaVar(Schema valueSchema, String variableName, JInvocation getValueType) {
        if (!useGenericTypes) {
            return null;
        }
        if (SchemaMapper.isComplexType(valueSchema) || SchemaMapper.isNamedType(valueSchema)) {
            JVar schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class), getVariableName(variableName),
                    getValueType);
            registerSchema(valueSchema, schemaVar);
            return schemaVar;
        } else {
            return null;
        }
    }

    private JVar declareSchemaVariableForRecordField(final String name, final Schema schema, JVar schemaVar) {
        return declareSchemaVar(schema, name + "Field", schemaVar.invoke("getField").arg(name).invoke("schema"));
    }

    private boolean methodAlreadyDefined(final Schema schema, boolean read) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException(
                    "Methods are defined only for records, not for " + schema.getType());
        }

        return read ? deserializeMethodMap.containsKey(schema.getFullName())
                : skipMethodMap.containsKey(schema.getFullName());
    }

    private JMethod getMethod(final Schema schema, boolean read) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException(
                    "Methods are defined only for records, not for " + schema.getType());
        }
        if (methodAlreadyDefined(schema, read)) {
            return read ? deserializeMethodMap.get(schema.getFullName()) : skipMethodMap.get(schema.getFullName());
        }
        throw new FastDeserializerGeneratorException("No method for schema: " + schema.getFullName());
    }

    private JMethod createMethod(final Schema schema, boolean read) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException(
                    "Methods are defined only for records, not for " + schema.getType());
        }
        if (methodAlreadyDefined(schema, read)) {
            throw new FastDeserializerGeneratorException("Method already exists for: " + schema.getFullName());
        }

        JMethod method = deserializerClass.method(JMod.PUBLIC, read
                ? schemaMapper.classFromSchema(schema)
                : codeModel.VOID,
                "deserialize" + schema.getName() + nextRandomInt());

        method._throws(IOException.class);
        method.param(Decoder.class, DECODER);

        if (read) {
            deserializeMethodMap.put(schema.getFullName(), method);
        } else {
            skipMethodMap.put(schema.getFullName(), method);
        }

        return method;
    }

    private void registerSchema(final Schema writerSchema, JVar schemaVar) {
        if ((Schema.Type.RECORD.equals(writerSchema.getType()) || Schema.Type.ENUM.equals(writerSchema.getType())
                || Schema.Type.ARRAY.equals(writerSchema.getType())) && schemaNotRegistered(writerSchema)) {
            schemaMap.put(getSchemaId(writerSchema), writerSchema);

            schemaMapMethod.body().invoke(schemaMapField, "put").arg(JExpr.lit(getSchemaId(writerSchema)))
                    .arg(schemaVar);
        }
    }

    private boolean schemaNotRegistered(final Schema schema) {
        return !schemaMap.containsKey(getSchemaId(schema));
    }

    private JInvocation getSchemaExpr(Schema schema) {
        if (useGenericTypes) {
            return schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema)));
        } else {
            return null;
        }
    }
}
