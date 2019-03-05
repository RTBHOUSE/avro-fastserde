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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.io.parsing.Symbol;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;

import com.sun.codemodel.JArray;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDoLoop;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JInvocation;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JStatement;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;

public class FastDeserializerGenerator<T> extends FastDeserializerGeneratorBase<T> {

    private static final String DECODER = "decoder";

    private boolean useGenericTypes;
    private JMethod schemaMapMethod;
    private JFieldVar schemaMapField;
    private Map<Integer, Schema> schemaMap = new HashMap<>();
    private Map<Integer, JVar> schemaVarMap = new HashMap<>();
    private Map<String, JMethod> deserializeMethodMap = new HashMap<>();
    private Map<String, JMethod> skipMethodMap = new HashMap<>();
    private Map<JMethod, Set<Class<? extends Exception>>> exceptionFromMethodMap = new HashMap<>();
    private SchemaAssistant schemaAssistant;

    FastDeserializerGenerator(boolean useGenericTypes, Schema writer, Schema reader, File destination,
            ClassLoader classLoader,
            String compileClassPath) {
        super(writer, reader, destination, classLoader, compileClassPath);
        this.useGenericTypes = useGenericTypes;
        this.schemaAssistant = new SchemaAssistant(codeModel, useGenericTypes);
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

            JClass readerSchemaClass = schemaAssistant.classFromSchema(reader);
            JClass writerSchemaClass = schemaAssistant.classFromSchema(aliasedWriterSchema);

            deserializerClass._implements(codeModel.ref(FastDeserializer.class).narrow(writerSchemaClass));
            JMethod deserializeMethod = deserializerClass.method(JMod.PUBLIC, readerSchemaClass, "deserialize");

            JBlock topLevelDeserializeBlock = new JBlock();

            switch (aliasedWriterSchema.getType()) {
            case RECORD:
                processRecord(readerSchemaVar, aliasedWriterSchema.getName(), aliasedWriterSchema, reader,
                        topLevelDeserializeBlock, fieldAction, JBlock::_return);
                break;
            case ARRAY:
                processArray(readerSchemaVar,  "array", aliasedWriterSchema, reader,
                        topLevelDeserializeBlock, fieldAction, JBlock::_return);
                break;
            case MAP:
                processMap(readerSchemaVar, "map", aliasedWriterSchema, reader,
                        topLevelDeserializeBlock, fieldAction, JBlock::_return);
                break;
            default:
                throw new FastDeserializerGeneratorException(
                        "Incorrect top-level writer schema: " + aliasedWriterSchema.getType());
            }

            if (schemaAssistant.getExceptionsFromStringable().isEmpty()) {
                assignBlockToBody(deserializeMethod, topLevelDeserializeBlock);
            } else {
                JTryBlock tryBlock = deserializeMethod.body()._try();
                assignBlockToBody(tryBlock, topLevelDeserializeBlock);

                for (Class<? extends Exception> classException : schemaAssistant.getExceptionsFromStringable()) {
                    JCatchBlock catchBlock = tryBlock._catch(codeModel.ref(classException));
                    JVar exceptionVar = catchBlock.param("e");
                    catchBlock.body()._throw(JExpr._new(codeModel.ref(AvroRuntimeException.class)).arg(exceptionVar));
                }
            }

            deserializeMethod._throws(codeModel.ref(IOException.class));
            deserializeMethod.param(Decoder.class, DECODER);

            Class<FastDeserializer<T>> clazz = compileClass(className);
            return clazz.getConstructor(Schema.class).newInstance(reader);
        } catch (JClassAlreadyExistsException e) {
            throw new FastDeserializerGeneratorException("Class: " + className + " already exists");
        } catch (Exception e) {
            throw new FastDeserializerGeneratorException(e);
        }
    }

    private void processComplexType(JVar fieldSchemaVar, String name, Schema schema, Schema readerFieldSchema,
            JBlock methodBody, FieldAction action, BiConsumer<JBlock, JExpression> putExpressionIntoParent) {
        switch (schema.getType()) {
        case RECORD:
            processRecord(fieldSchemaVar, schema.getName(), schema, readerFieldSchema, methodBody, action,
                    putExpressionIntoParent);
            break;
        case ARRAY:
            processArray(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action, putExpressionIntoParent);
            break;
        case MAP:
            processMap(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action, putExpressionIntoParent);
            break;
        case UNION:
            processUnion(fieldSchemaVar, name, schema, readerFieldSchema, methodBody, action, putExpressionIntoParent);
            break;
        default:
            throw new FastDeserializerGeneratorException("Incorrect complex type: " + action.getType());
        }
    }

    private void processSimpleType(Schema schema, JBlock methodBody, FieldAction action,
            BiConsumer<JBlock, JExpression> putExpressionIntoParent) {
        switch (schema.getType()) {
        case ENUM:
            processEnum(schema, methodBody, action, putExpressionIntoParent);
            break;
        case FIXED:
            processFixed(schema, methodBody, action, putExpressionIntoParent);
            break;
        default:
            processPrimitive(schema, methodBody, action, putExpressionIntoParent);
        }
    }

    private void processRecord(JVar recordSchemaVar, String recordName, final Schema recordWriterSchema,
            final Schema recordReaderSchema, JBlock parentBody, FieldAction recordAction,
            BiConsumer<JBlock, JExpression> putRecordIntoParent) {

        ListIterator<Symbol> actionIterator = actionIterator(recordAction);

        if (methodAlreadyDefined(recordWriterSchema, recordAction.getShouldRead())) {
            JMethod method = getMethod(recordWriterSchema, recordAction.getShouldRead());
            updateActualExceptions(method);
            JExpression readingExpression = JExpr.invoke(method).arg(JExpr.direct(DECODER));
            if (recordAction.getShouldRead()) {
                putRecordIntoParent.accept(parentBody, readingExpression);
            } else {
                parentBody.add((JStatement) readingExpression);
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
            Set<String> fieldNamesSet = recordWriterSchema.getFields()
                    .stream().map(Schema.Field::name).collect(Collectors.toSet());
            for (Schema.Field readerField : recordReaderSchema.getFields()) {
                if (!fieldNamesSet.contains(readerField.name())) {
                    forwardToExpectedDefault(actionIterator);
                    seekFieldAction(true, readerField, actionIterator);
                }
            }
            return;
        }

        JMethod method = createMethod(recordWriterSchema, recordAction.getShouldRead());

        Set<Class<? extends Exception>> exceptionsOnHigherLevel = schemaAssistant.getExceptionsFromStringable();
        schemaAssistant.resetExceptionsFromStringable();

        if (recordAction.getShouldRead()) {
            putRecordIntoParent.accept(parentBody, JExpr.invoke(method).arg(JExpr.direct(DECODER)));
        } else {
            parentBody.invoke(method).arg(JExpr.direct(DECODER));
        }

        final JBlock methodBody = method.body();

        final JVar result;
        if (recordAction.getShouldRead()) {
            JClass recordClass = schemaAssistant.classFromSchema(recordWriterSchema);
            JInvocation newRecord = JExpr._new(schemaAssistant.classFromSchema(recordWriterSchema, false));
            if (useGenericTypes) {
                newRecord = newRecord.arg(schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(recordWriterSchema))));
            }
            result = methodBody.decl(recordClass, recordName, newRecord);
        } else {
            result = null;
        }

        for (Schema.Field field : recordWriterSchema.getFields()) {

            FieldAction action = seekFieldAction(recordAction.getShouldRead(), field, actionIterator);
            if (action.getSymbol() == END_SYMBOL) {
                break;
            }

            Schema readerFieldSchema = null;
            JVar fieldSchemaVar = null;
            BiConsumer<JBlock, JExpression> putExpressionInRecord = null;
            if (action.getShouldRead()) {
                Schema.Field readerField = recordReaderSchema.getField(field.name());
                readerFieldSchema = readerField.schema();
                final int readerFieldPos = readerField.pos();
                putExpressionInRecord = (block, expression) -> block.invoke(result, "put")
                        .arg(JExpr.lit(readerFieldPos)).arg(expression);
                if (useGenericTypes) {
                    fieldSchemaVar = declareSchemaVar(field.schema(), field.name(),
                            recordSchemaVar.invoke("getField").arg(field.name()).invoke("schema"));
                }
            }

            if (SchemaAssistant.isComplexType(field.schema())) {
                processComplexType(fieldSchemaVar, field.name(), field.schema(), readerFieldSchema, methodBody, action,
                        putExpressionInRecord);
            } else {
                processSimpleType(field.schema(), methodBody, action, putExpressionInRecord);

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
                    JExpression value = parseDefaultValue(readerField.schema(), readerField.defaultValue(), methodBody,
                            schemaVar, readerField.name());
                    methodBody.invoke(result, "put").arg(JExpr.lit(readerField.pos())).arg(value);
                }
            }
        }

        if (recordAction.getShouldRead()) {
            methodBody._return(result);
        }
        exceptionFromMethodMap.put(method, schemaAssistant.getExceptionsFromStringable());
        schemaAssistant.setExceptionsFromStringable(exceptionsOnHigherLevel);
        updateActualExceptions(method);
    }

    private void updateActualExceptions(JMethod method) {
        Set<Class<? extends Exception>> exceptionFromMethod = exceptionFromMethodMap.get(method);
        for (Class<? extends Exception> exceptionClass : exceptionFromMethod) {
            method._throws(exceptionClass);
            schemaAssistant.getExceptionsFromStringable().add(exceptionClass);
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

        if (SchemaAssistant.isComplexType(schema)) {
            JClass defaultValueClass = schemaAssistant.classFromSchema(schema, false);
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
                    if (SchemaAssistant.hasStringableKey(schema)) {
                        mapKeyExpr = JExpr._new(schemaAssistant.keyClassFromMapSchema(schema)).arg(mapEntry.getKey());
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
                return schemaAssistant.getEnumValueByName(schema, JExpr.lit(defaultValue.getTextValue()),
                        getSchemaExpr(schema));
            case FIXED:
                JArray fixedBytesArray = JExpr.newArray(codeModel.BYTE);
                for (char b : defaultValue.getTextValue().toCharArray()) {
                    fixedBytesArray.add(JExpr.lit((byte) b));
                }
                return schemaAssistant.getFixedValue(schema, fixedBytesArray, getSchemaExpr(schema));
            case BYTES:
                JArray bytesArray = JExpr.newArray(codeModel.BYTE);
                for (byte b : defaultValue.getTextValue().getBytes()) {
                    bytesArray.add(JExpr.lit(b));
                }
                return codeModel.ref(ByteBuffer.class).staticInvoke("wrap").arg(bytesArray);
            case STRING:
                return schemaAssistant.getStringableValue(schema, JExpr.lit(defaultValue.getTextValue()));
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

    private void processUnion(JVar unionSchemaVar, final String name, final Schema unionSchema,
            final Schema readerUnionSchema, JBlock body, FieldAction action,
            BiConsumer<JBlock, JExpression> putValueIntoParent) {
        JVar unionIndex = body.decl(codeModel.INT, getVariableName("unionIndex"),
                JExpr.direct(DECODER + ".readIndex()"));
        JConditional ifBlock = null;
        for (int i = 0; i < unionSchema.getTypes().size(); i++) {
            Schema optionSchema = unionSchema.getTypes().get(i);
            Schema readerOptionSchema = null;
            FieldAction unionAction;

            if (Schema.Type.NULL.equals(optionSchema.getType())) {
                JBlock nullReadBlock = body._if(unionIndex.eq(JExpr.lit(i)))._then().block();
                nullReadBlock.directStatement(DECODER + ".readNull();");
                if (action.getShouldRead()) {
                    putValueIntoParent.accept(nullReadBlock, JExpr._null());
                }
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
            final JBlock thenBlock = ifBlock._then();

            JVar optionSchemaVar = null;
            if (useGenericTypes && unionAction.getShouldRead()) {
                JInvocation optionSchemaExpression = unionSchemaVar.invoke("getTypes").invoke("get").arg(JExpr.lit(i));
                optionSchemaVar = declareSchemaVar(optionSchema, name + "OptionSchema", optionSchemaExpression);
            }

            if (SchemaAssistant.isComplexType(optionSchema)) {
                String optionName = name + "Option";
                if (Schema.Type.UNION.equals(optionSchema.getType())) {
                    throw new FastDeserializerGeneratorException("Union cannot be sub-type of union!");
                }
                processComplexType(optionSchemaVar, optionName, optionSchema, readerOptionSchema, thenBlock,
                        unionAction, putValueIntoParent);
            } else {
                processSimpleType(optionSchema, thenBlock, unionAction, putValueIntoParent);
            }
        }
    }

    private void processArray(JVar arraySchemaVar, final String name, final Schema arraySchema,
            final Schema readerArraySchema, JBlock parentBody, FieldAction action,
            BiConsumer<JBlock, JExpression> putArrayIntoParent) {

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

        final JVar arrayVar = action.getShouldRead() ? declareValueVar(name, arraySchema, parentBody) : null;
        JVar chunkLen = parentBody.decl(codeModel.LONG, getVariableName("chunkLen"),
                JExpr.direct(DECODER + ".readArrayStart()"));

        JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        JClass arrayClass = schemaAssistant.classFromSchema(arraySchema, false);

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

        JVar elementSchemaVar = null;
        BiConsumer<JBlock, JExpression> putValueInArray = null;
        if (action.getShouldRead()) {
            putValueInArray = (block, expression) -> block.invoke(arrayVar, "add").arg(expression);
            if (useGenericTypes) {
                elementSchemaVar = declareSchemaVar(arraySchema.getElementType(), name + "ArrayElemSchema",
                        arraySchemaVar.invoke("getElementType"));
            }
        }

        if (SchemaAssistant.isComplexType(arraySchema.getElementType())) {
            String elemName = name + "Elem";
            Schema readerArrayElementSchema = null;
            if (action.getShouldRead()) {
                readerArrayElementSchema = readerArraySchema.getElementType();
            }
            processComplexType(elementSchemaVar, elemName, arraySchema.getElementType(), readerArrayElementSchema,
                    forBody, action, putValueInArray);
        } else {
            processSimpleType(arraySchema.getElementType(), forBody, action, putValueInArray);
        }
        doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".arrayNext()"));

        if (action.getShouldRead()) {
            putArrayIntoParent.accept(parentBody, arrayVar);
        }
    }

    private void processMap(JVar mapSchemaVar, final String name, final Schema mapSchema, final Schema readerMapSchema,
            JBlock parentBody, FieldAction action, BiConsumer<JBlock, JExpression> putMapIntoParent) {

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

        final JVar mapVar = action.getShouldRead() ? declareValueVar(name, mapSchema, parentBody) : null;
        JVar chunkLen = parentBody.decl(codeModel.LONG, getVariableName("chunkLen"),
                JExpr.direct(DECODER + ".readMapStart()"));

        JConditional conditional = parentBody._if(chunkLen.gt(JExpr.lit(0)));
        JBlock ifBlock = conditional._then();

        if (action.getShouldRead()) {
            ifBlock.assign(mapVar, JExpr._new(schemaAssistant.classFromSchema(mapSchema, false)));
            JBlock elseBlock = conditional._else();
            elseBlock.assign(mapVar, codeModel.ref(Collections.class).staticInvoke("emptyMap"));
        }

        JDoLoop doLoop = ifBlock._do(chunkLen.gt(JExpr.lit(0)));
        JForLoop forLoop = doLoop.body()._for();
        JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(chunkLen));
        forLoop.update(counter.incr());
        JBlock forBody = forLoop.body();

        JClass keyClass = schemaAssistant.keyClassFromMapSchema(mapSchema);
        JExpression keyValueExpression = JExpr.direct(DECODER + ".readString()");
        if (SchemaAssistant.hasStringableKey(mapSchema)) {
            keyValueExpression = JExpr._new(keyClass).arg(keyValueExpression);
        }

        JVar key = forBody.decl(keyClass, getVariableName("key"), keyValueExpression);
        JVar mapValueSchemaVar = null;
        if (action.getShouldRead() && useGenericTypes) {
            mapValueSchemaVar = declareSchemaVar(mapSchema.getValueType(), name + "MapValueSchema",
                    mapSchemaVar.invoke("getValueType"));
        }
        BiConsumer<JBlock, JExpression> putValueInMap = null;
        if (action.getShouldRead()) {
            putValueInMap = (block, expression) -> block.invoke(mapVar, "put").arg(key).arg(expression);
        }
        if (SchemaAssistant.isComplexType(mapSchema.getValueType())) {
            String valueName = name + "Value";
            Schema readerMapValueSchema = null;
            if (action.getShouldRead()) {
                readerMapValueSchema = readerMapSchema.getValueType();
            }
            processComplexType(mapValueSchemaVar, valueName, mapSchema.getValueType(), readerMapValueSchema, forBody,
                    action, putValueInMap);
        } else {
            processSimpleType(mapSchema.getValueType(), forBody, action, putValueInMap);
        }
        doLoop.body().assign(chunkLen, JExpr.direct(DECODER + ".mapNext()"));
        if (action.getShouldRead()) {
            putMapIntoParent.accept(parentBody, mapVar);
        }
    }

    private void processFixed(final Schema schema, JBlock body, FieldAction action,
            BiConsumer<JBlock, JExpression> putFixedIntoParent) {
        if (action.getShouldRead()) {
            JVar fixedBuffer = body.decl(codeModel.ref(byte[].class), getVariableName(schema.getName()))
                    .init(JExpr.direct(" new byte[" + schema.getFixedSize() + "]"));

            body.directStatement(DECODER + ".readFixed(" + fixedBuffer.name() + ");");

            JInvocation createFixed = JExpr._new(schemaAssistant.classFromSchema(schema));
            if (useGenericTypes)
                createFixed = createFixed.arg(getSchemaExpr(schema));

            putFixedIntoParent.accept(body, createFixed.arg(fixedBuffer));
        } else {
            body.directStatement(DECODER + ".skipFixed(" + schema.getFixedSize() + ");");
        }
    }

    private void processEnum(final Schema schema, final JBlock body, FieldAction action,
            BiConsumer<JBlock, JExpression> putEnumIntoParent) {

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
                newEnum = schemaAssistant.getEnumValueByIndex(schema, enumValueExpr, getSchemaExpr(schema));
            } else {
                JVar enumIndex = body.decl(codeModel.INT, getVariableName("enumIndex"), enumValueExpr);
                JClass enumClass = schemaAssistant.classFromSchema(schema);
                newEnum = body.decl(enumClass, getVariableName("enumValue"), JExpr._null());

                for (int i = 0; i < enumAdjustAction.adjustments.length; i++) {
                    JExpression ithVal = schemaAssistant
                            .getEnumValueByIndex(schema, JExpr.lit((Integer) enumAdjustAction.adjustments[i]),
                                    getSchemaExpr(schema));
                    body._if(enumIndex.eq(JExpr.lit(i)))._then().assign((JVar) newEnum, ithVal);
                }
            }
            putEnumIntoParent.accept(body, newEnum);
        } else {
            body.directStatement(DECODER + ".readEnum();");
        }

    }

    private void processPrimitive(final Schema schema, JBlock body, FieldAction action,
            BiConsumer<JBlock, JExpression> putValueIntoParent) {

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
            if (schema.getType().equals(Schema.Type.STRING) && SchemaAssistant.isStringable(schema)) {
                primitiveValueExpression = JExpr._new(schemaAssistant.classFromSchema(schema))
                        .arg(primitiveValueExpression);
            }
            putValueIntoParent.accept(body, primitiveValueExpression);
        } else {
            body.directStatement(DECODER + "." + readFunction + ";");
        }
    }

    private JVar declareSchemaVariableForRecordField(final String name, final Schema schema, JVar schemaVar) {
        return declareSchemaVar(schema, name + "Field", schemaVar.invoke("getField").arg(name).invoke("schema"));
    }

    private JVar declareValueVar(final String name, final Schema schema, JBlock block) {
        if (SchemaAssistant.isComplexType(schema)) {
            return block.decl(schemaAssistant.classFromSchema(schema), getVariableName(StringUtils.uncapitalize(name)), JExpr._null());
        } else {
            throw new FastDeserializerGeneratorException("Only complex types allowed!");
        }
    }

    private JVar declareSchemaVar(Schema valueSchema, String variableName, JInvocation getValueType) {
        if (!useGenericTypes) {
            return null;
        }
        if (SchemaAssistant.isComplexType(valueSchema) || Schema.Type.ENUM.equals(valueSchema.getType())) {
            int schemaId = getSchemaId(valueSchema);
            if (schemaVarMap.get(schemaId) != null) {
                return schemaVarMap.get(schemaId);
            } else {
                JVar schemaVar = schemaMapMethod.body().decl(codeModel.ref(Schema.class),
                        getVariableName(StringUtils.uncapitalize(variableName)), getValueType);
                registerSchema(valueSchema, schemaId, schemaVar);
                schemaVarMap.put(schemaId, schemaVar);
                return schemaVar;
            }
        } else {
            return null;
        }
    }

    private void registerSchema(final Schema writerSchema, JVar schemaVar) {
        registerSchema(writerSchema, getSchemaId(writerSchema), schemaVar);
    }

    private void registerSchema(final Schema writerSchema, int schemaId, JVar schemaVar) {
        if ((Schema.Type.RECORD.equals(writerSchema.getType()) || Schema.Type.ENUM.equals(writerSchema.getType())
                || Schema.Type.ARRAY.equals(writerSchema.getType())) && schemaNotRegistered(writerSchema)) {
            schemaMap.put(schemaId, writerSchema);
            schemaMapMethod.body().invoke(schemaMapField, "put").arg(JExpr.lit(schemaId)).arg(schemaVar);
        }
    }

    private boolean schemaNotRegistered(final Schema schema) {
        return !schemaMap.containsKey(getSchemaId(schema));
    }

    private boolean methodAlreadyDefined(final Schema schema, boolean read) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException(
                    "Methods are defined only for records, not for " + schema.getType());
        }

        return (read ? deserializeMethodMap : skipMethodMap).containsKey(schema.getFullName());
    }

    private JMethod getMethod(final Schema schema, boolean read) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException(
                    "Methods are defined only for records, not for " + schema.getType());
        }
        if (!methodAlreadyDefined(schema, read)) {
            throw new FastDeserializerGeneratorException("No method for schema: " + schema.getFullName());
        }
        return (read ? deserializeMethodMap : skipMethodMap).get(schema.getFullName());
    }

    private JMethod createMethod(final Schema schema, boolean read) {
        if (!Schema.Type.RECORD.equals(schema.getType())) {
            throw new FastDeserializerGeneratorException(
                    "Methods are defined only for records, not for " + schema.getType());
        }
        if (methodAlreadyDefined(schema, read)) {
            throw new FastDeserializerGeneratorException("Method already exists for: " + schema.getFullName());
        }

        JMethod method = deserializerClass.method(JMod.PUBLIC,
                read ? schemaAssistant.classFromSchema(schema) : codeModel.VOID,
                getVariableName("deserialize" + schema.getName()));

        method._throws(IOException.class);
        method.param(Decoder.class, DECODER);

        (read ? deserializeMethodMap : skipMethodMap).put(schema.getFullName(), method);

        return method;
    }

    private JInvocation getSchemaExpr(Schema schema) {
        return useGenericTypes ? schemaMapField.invoke("get").arg(JExpr.lit(getSchemaId(schema))) : null;
    }
}
