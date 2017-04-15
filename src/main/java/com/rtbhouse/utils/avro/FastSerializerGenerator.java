package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Encoder;
import org.apache.commons.lang3.StringUtils;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JForEach;
import com.sun.codemodel.JForLoop;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JVar;

public class FastSerializerGenerator<T> extends FastSerializerGeneratorBase<T> {

    private static final String ENCODER = "encoder";

    private boolean useGenericTypes;
    private Map<String, JMethod> serializeMethodMap = new HashMap<>();

    public FastSerializerGenerator(boolean useGenericTypes, Schema schema, File destination, ClassLoader classLoader,
            String compileClassPath) {
        super(schema, destination, classLoader, compileClassPath);
        this.useGenericTypes = useGenericTypes;
    }

    @Override
    public FastSerializer<T> generateSerializer() {
        final String className = getClassName(schema, useGenericTypes ? "Generic" : "Specific");
        final JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

        try {
            serializerClass = classPackage._class(className);

            final JMethod serializeMethod = serializerClass.method(JMod.PUBLIC, void.class, "serialize");
            JVar serializeMethodParam;
            if (Schema.Type.RECORD.equals(schema.getType())) {
                serializerClass._implements(codeModel.ref(FastSerializer.class)
                        .narrow(useGenericTypes ? codeModel.ref(GenericData.Record.class)
                                : codeModel.ref(schema.getFullName())));

                serializeMethodParam = serializeMethod.param(
                        useGenericTypes ? codeModel.ref(GenericData.Record.class) : codeModel.ref(schema.getFullName()),
                        "data");
                processRecord(schema, serializeMethodParam, serializeMethod.body());
            } else if (Schema.Type.ARRAY.equals(schema.getType())) {
                serializerClass._implements(codeModel.ref(FastSerializer.class).narrow(
                        (useGenericTypes ? codeModel.ref(GenericData.Array.class) : codeModel.ref(List.class))
                                .narrow(classFromArraySchemaElementType(schema))));

                serializeMethodParam = serializeMethod
                        .param((useGenericTypes ? codeModel.ref(GenericData.Array.class) : codeModel.ref(List.class))
                                .narrow(classFromArraySchemaElementType(schema)), "data");
                processArray(schema, serializeMethodParam, serializeMethod.body());
            } else if (Schema.Type.MAP.equals(schema.getType())) {
                serializerClass._implements(codeModel.ref(FastSerializer.class).narrow(
                        codeModel.ref(Map.class).narrow(codeModel.ref(String.class),
                                classFromMapSchemaElementType(schema))));

                serializeMethodParam = serializeMethod.param(
                        codeModel.ref(Map.class).narrow(codeModel.ref(String.class),
                                classFromMapSchemaElementType(schema)),
                        "data");
                processMap(schema, serializeMethodParam, serializeMethod.body());
            } else {
                throw new FastSerializerGeneratorException("Unsupported input schema type: "
                        + schema.getType());
            }
            serializeMethod.param(codeModel.ref(Encoder.class), ENCODER);
            serializeMethod._throws(codeModel.ref(IOException.class));

            final Class<FastSerializer<T>> clazz = compileClass(className);
            return clazz.newInstance();
        } catch (JClassAlreadyExistsException e) {
            throw new FastSerializerGeneratorException("Class: " + className + " already exists");
        } catch (Exception e) {
            throw new FastSerializerGeneratorException(e);
        }
    }

    private void processRecord(final Schema recordSchema, JVar containerVariable, JBlock body) {
        JMethod method;
        if (doesNotContainMethod(recordSchema)) {
            method = createMethod(recordSchema);
            body.invoke(getMethod(recordSchema)).arg(containerVariable).arg(JExpr.direct(ENCODER));

            body = method.body();
        } else {
            body.invoke(getMethod(recordSchema)).arg(containerVariable).arg(JExpr.direct(ENCODER));
            return;
        }

        containerVariable = method.listParams()[0];

        for (Schema.Field field : recordSchema.getFields()) {

            JVar containerVar = declareContainerVariableForSchemaInBlock(getVariableName(field.name()), field.schema(),
                    body);

            if (Schema.Type.RECORD.equals(field.schema().getType())) {
                containerVar.init(JExpr.cast(useGenericTypes ? codeModel.ref(GenericData.Record.class)
                        : codeModel.ref(field.schema().getFullName()),
                        JExpr.invoke(containerVariable, "get").arg(JExpr.lit(field.pos()))));
                processRecord(field.schema(), containerVar, body);
            } else if (Schema.Type.ARRAY.equals(field.schema().getType())) {
                containerVar.init(JExpr.cast(
                        codeModel.ref(List.class).narrow(classFromArraySchemaElementType(field.schema())),
                        JExpr.invoke(containerVariable, "get").arg(JExpr.lit(field.pos()))));
                processArray(field.schema(), containerVar, body);
            } else if (Schema.Type.MAP.equals(field.schema().getType())) {
                containerVar.init(JExpr.cast(codeModel.ref(Map.class).narrow(String.class)
                        .narrow(classFromMapSchemaElementType(field.schema())),
                        JExpr.invoke(containerVariable, "get").arg(JExpr.lit(field.pos()))));
                processMap(field.schema(), containerVar, body);
            } else if (Schema.Type.UNION.equals(field.schema().getType())) {
                processUnion(containerVariable, recordSchema, field.schema(), field, body);
            } else if (Schema.Type.ENUM.equals((field.schema().getType()))) {
                processEnum(containerVariable, recordSchema, field, body);
            } else if (Schema.Type.FIXED.equals((field.schema().getType()))) {
                processFixed(containerVariable, recordSchema, field, body);
            } else {
                processPrimitive(containerVariable, recordSchema, field.schema(), field, body);
            }
        }
    }

    private void processArray(final Schema arraySchema, JVar containerVariable, JBlock body) {
        final JClass arrayType = codeModel.ref(List.class).narrow(classFromArraySchemaElementType(arraySchema));
        body.invoke(JExpr.direct(ENCODER), "writeArrayStart");

        final JExpression emptyArrayCondition = containerVariable.eq(JExpr._null())
                .cor(JExpr.invoke(JExpr.cast(arrayType, containerVariable), "size").eq(JExpr.lit(0)));

        final JConditional emptyArrayIf = body._if(emptyArrayCondition);
        final JBlock emptyArrayBlock = emptyArrayIf._then();
        emptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
        emptyArrayBlock.invoke(JExpr.direct(ENCODER), "writeArrayEnd");

        final JBlock nonEmptyArrayBlock = emptyArrayIf._else();
        nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount")
                .arg(JExpr.invoke(JExpr.cast(arrayType, containerVariable), "size"));
        final JForLoop forLoop = nonEmptyArrayBlock._for();
        final JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(JExpr.invoke(JExpr.cast(arrayType, containerVariable), "size")));
        forLoop.update(counter.incr());
        final JBlock forBody = forLoop.body();
        forBody.invoke(JExpr.direct(ENCODER), "startItem");

        final Schema elementSchema = arraySchema.getElementType();

        if (Schema.Type.RECORD.equals(elementSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(elementSchema.getName(), elementSchema,
                    forBody);
            forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayType, containerVariable), "get").arg(counter));
            processRecord(elementSchema, containerVar, forBody);
        } else if (Schema.Type.ARRAY.equals(elementSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(elementSchema.getName(), elementSchema,
                    forBody);
            forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayType, containerVariable), "get").arg(counter));
            processArray(elementSchema, containerVar, forBody);
        } else if (Schema.Type.MAP.equals(elementSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(elementSchema.getName(), elementSchema,
                    forBody);
            forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayType, containerVariable), "get").arg(counter));
            processMap(elementSchema, containerVar, forBody);
        } else if (Schema.Type.ENUM.equals(elementSchema.getType())) {
            processEnum(containerVariable, arraySchema, counter, forBody);
        } else if (Schema.Type.FIXED.equals(elementSchema.getType())) {
            processFixed(containerVariable, arraySchema, counter, forBody);
        } else if (Schema.Type.UNION.equals(elementSchema.getType())) {
            processUnion(containerVariable, arraySchema, elementSchema, counter, forBody);
        } else {
            processPrimitive(containerVariable, arraySchema, elementSchema, counter, forBody);
        }

        nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "writeArrayEnd");

    }

    private void processMap(final Schema mapSchema, JVar containerVariable, JBlock body) {
        final JClass mapType = codeModel.ref(Map.class).narrow(String.class)
                .narrow(classFromMapSchemaElementType(mapSchema));

        body.invoke(JExpr.direct(ENCODER), "writeMapStart");

        final JExpression emptyMapCondition = containerVariable.eq(JExpr._null())
                .cor(JExpr.invoke(JExpr.cast(mapType, containerVariable), "size").eq(JExpr.lit(0)));
        final JConditional emptyMapIf = body._if(emptyMapCondition);
        final JBlock emptyMapBlock = emptyMapIf._then();
        emptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
        emptyMapBlock.invoke(JExpr.direct(ENCODER), "writeMapEnd");

        final JBlock nonEmptyMapBlock = emptyMapIf._else();
        nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount")
                .arg(JExpr.invoke(JExpr.cast(mapType, containerVariable), "size"));
        final JForEach forEachLoop = nonEmptyMapBlock.forEach(codeModel.ref(String.class), getVariableName("key"),
                JExpr.invoke(JExpr.cast(mapType, containerVariable), "keySet"));
        final JBlock forBody = forEachLoop.body();
        forBody.invoke(JExpr.direct(ENCODER), "startItem");

        final Schema valueSchema = mapSchema.getValueType();

        if (Schema.Type.RECORD.equals(valueSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema,
                    forBody);
            forBody.assign(containerVar,
                    JExpr.invoke(JExpr.cast(mapType, containerVariable), "get").arg(forEachLoop.var()));
            forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(forEachLoop.var());
            processRecord(valueSchema, containerVar, forBody);
        } else if (Schema.Type.ARRAY.equals(valueSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema,
                    forBody);
            forBody.assign(containerVar,
                    JExpr.invoke(JExpr.cast(mapType, containerVariable), "get").arg(forEachLoop.var()));
            forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(forEachLoop.var());
            processArray(valueSchema, containerVar, forBody);
        } else if (Schema.Type.MAP.equals(valueSchema.getType())) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema,
                    forBody);
            forBody.assign(containerVar,
                    JExpr.invoke(JExpr.cast(mapType, containerVariable), "get").arg(forEachLoop.var()));
            forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(forEachLoop.var());
            processMap(valueSchema, containerVar, forBody);
        } else if (Schema.Type.ENUM.equals(valueSchema.getType())) {
            processEnum(containerVariable, forEachLoop.var(), mapSchema, forBody);
        } else if (Schema.Type.FIXED.equals(valueSchema.getType())) {
            processFixed(containerVariable, forEachLoop.var(), mapSchema, forBody);
        } else if (Schema.Type.UNION.equals(valueSchema.getType())) {
            processUnion(containerVariable, forEachLoop.var(), mapSchema, valueSchema, forBody);
        } else {
            processPrimitive(containerVariable, forEachLoop.var(), mapSchema, valueSchema, forBody);
        }

        nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "writeMapEnd");
    }

    private void processUnion(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema unionSchema,
            JBlock body) {
        processUnion(containerVariable, keyVariable, containerSchema, unionSchema, null, null, body);
    }

    private void processUnion(JVar containerVariable, final Schema containerSchema, final Schema unionSchema,
            JVar counterVariable,
            JBlock body) {
        processUnion(containerVariable, null, containerSchema, unionSchema, counterVariable, null, body);
    }

    private void processUnion(JVar containerVariable, final Schema containerSchema, final Schema unionSchema,
            final Schema.Field field,
            JBlock body) {
        processUnion(containerVariable, null, containerSchema, unionSchema, null, field, body);
    }

    private void processUnion(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema unionSchema,
            JVar counterVariable, Schema.Field field, JBlock body) {

        JVar unionVariable = null;
        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            unionVariable = body.decl(codeModel.ref(Object.class), getVariableName(field.name()), containerVariable
                    .invoke("get").arg(JExpr.lit(field.pos())));
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            unionVariable = body.decl(codeModel.ref(Object.class), getVariableName(containerSchema.getName()),
                    containerVariable.invoke("get").arg(counterVariable));
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            unionVariable = body.decl(codeModel.ref(Object.class), getVariableName(containerSchema.getName()),
                    containerVariable.invoke("get").arg(keyVariable));

            body.invoke(JExpr.direct(ENCODER), "writeString").arg(keyVariable);
        }

        JConditional ifBlock = null;
        for (Schema schema : unionSchema.getTypes()) {

            if (Schema.Type.RECORD.equals(schema.getType())) {
                final JClass recordClass = useGenericTypes ? codeModel.ref(GenericData.Record.class)
                        : codeModel.ref(schema.getFullName());
                JExpression condition = unionVariable._instanceof(recordClass);
                if (useGenericTypes) {
                    condition = condition.cand(
                            JExpr.invoke(JExpr.lit(schema.getFullName()), "equals").arg(
                                    JExpr.invoke(JExpr.cast(recordClass, unionVariable),
                                            "getSchema").invoke("getFullName")));
                }

                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);

                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getFullName())));
                JVar recordVar = thenBlock.decl(recordClass,
                        getVariableName(schema.getName()),
                        JExpr.cast(recordClass, unionVariable));
                processRecord(schema, recordVar, thenBlock);
            }

            else if (Schema.Type.ENUM.equals(schema.getType())) {
                final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                        : codeModel.ref(schema.getFullName());
                JExpression condition = unionVariable._instanceof(enumClass);
                if (useGenericTypes) {
                    condition = condition.cand(
                            JExpr.invoke(JExpr.lit(schema.getFullName()), "equals").arg(
                                    JExpr.invoke(
                                            JExpr.cast(enumClass, unionVariable),
                                            "getSchema").invoke("getFullName")));
                }

                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);

                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getFullName())));
                JVar enumVar = thenBlock.decl(enumClass,
                        getVariableName(schema.getName()),
                        JExpr.cast(enumClass, unionVariable));
                processEnum(enumVar, schema, thenBlock);
            }

            else if (Schema.Type.FIXED.equals(schema.getType())) {
                final JClass fixedClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                        : codeModel.ref(schema.getFullName());
                JExpression condition = unionVariable._instanceof(fixedClass);
                if (useGenericTypes) {
                    condition = condition.cand(
                            JExpr.invoke(JExpr.lit(schema.getFullName()), "equals").arg(
                                    JExpr.invoke(
                                            JExpr.cast(fixedClass, unionVariable),
                                            "getSchema").invoke("getFullName")));
                }

                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);

                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getFullName())));
                JVar enumVar = thenBlock.decl(fixedClass,
                        getVariableName(schema.getName()),
                        JExpr.cast(fixedClass, unionVariable));
                processFixed(enumVar, schema, thenBlock);
            }

            else if (Schema.Type.ARRAY.equals(schema.getType())) {
                JExpression condition = unionVariable._instanceof(codeModel.ref(List.class));
                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);

                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getType().getName())));
                JVar arrayVar = thenBlock.decl(
                        codeModel.ref(List.class).narrow(classFromArraySchemaElementType(schema)),
                        getVariableName(schema.getName()),
                        JExpr.cast(codeModel.ref(List.class).narrow(classFromArraySchemaElementType(schema)),
                                unionVariable));
                processArray(schema, arrayVar, thenBlock);
            }

            else if (Schema.Type.MAP.equals(schema.getType())) {
                JExpression condition = unionVariable._instanceof(codeModel.ref(Map.class));
                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);

                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getType().getName())));
                JVar mapVar = thenBlock.decl(
                        codeModel.ref(Map.class).narrow(String.class).narrow(classFromMapSchemaElementType(schema)),
                        getVariableName(schema.getName()),
                        JExpr.cast(codeModel.ref(Map.class).narrow(String.class)
                                .narrow(classFromMapSchemaElementType(schema)), unionVariable));
                processMap(schema, mapVar, thenBlock);
            }

            else if (Schema.Type.NULL.equals(schema.getType())) {
                JExpression condition = unionVariable.eq(JExpr._null());
                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getType().getName())));
                thenBlock.invoke(JExpr.direct(ENCODER), "writeNull");
            }

            else {
                JExpression condition = unionVariable._instanceof(classFromPrimitiveSchema(schema));
                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);

                JBlock thenBlock = ifBlock._then();

                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schema.getType().getName())));
                processPrimitive(unionVariable, schema, thenBlock);
            }
        }

    }

    private void processFixed(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JBlock body) {
        processFixed(containerVariable, keyVariable, containerSchema, null, null, body);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema,
            final Schema.Field readerField, JBlock body) {
        processFixed(containerVariable, null, containerSchema, null, readerField, body);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema, JVar counterVariable,
            JBlock body) {
        processFixed(containerVariable, null, containerSchema, counterVariable, null, body);
    }

    private void processFixed(JVar containerVariable, final Schema containerSchema, JBlock body) {
        processFixed(containerVariable, null, containerSchema, null, null, body);
    }

    private void processFixed(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JVar counterVariable, final Schema.Field field, JBlock body) {

        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            final JClass fixedClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(field.schema().getFullName());

            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(
                    JExpr.invoke(
                            JExpr.cast(fixedClass, containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                            "bytes"));
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            final JClass fixedClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(containerSchema.getElementType().getFullName());

            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(
                    JExpr.invoke(JExpr.cast(fixedClass, containerVariable.invoke("get").arg(counterVariable)),
                            "bytes"));
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            final JClass fixedClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(containerSchema.getValueType().getFullName());
            body.invoke(JExpr.direct(ENCODER), "writeString").arg(keyVariable);

            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(
                    JExpr.invoke(JExpr.cast(fixedClass, containerVariable.invoke("get").arg(keyVariable)),
                            "bytes"));
        } else if (Schema.Type.FIXED.equals(containerSchema.getType())) {
            body.invoke(JExpr.direct(ENCODER), "writeFixed").arg(containerVariable.invoke("bytes"));
        }
    }

    private void processEnum(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JBlock body) {
        processEnum(containerVariable, keyVariable, containerSchema, null, null, body);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema,
            final Schema.Field readerField, JBlock body) {
        processEnum(containerVariable, null, containerSchema, null, readerField, body);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema, JVar counterVariable,
            JBlock body) {
        processEnum(containerVariable, null, containerSchema, counterVariable, null, body);
    }

    private void processEnum(JVar containerVariable, final Schema containerSchema, JBlock body) {
        processEnum(containerVariable, null, containerSchema, null, null, body);
    }

    private void processEnum(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            JVar counterVariable, final Schema.Field field, JBlock body) {

        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(field.schema().getFullName());

            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(JExpr.invoke(
                                JExpr.cast(enumClass, containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                                "getSchema"), "getEnumOrdinal")
                                .arg(JExpr.invoke(
                                        JExpr.cast(enumClass,
                                                containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                                        "toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(
                                JExpr.cast(enumClass, containerVariable.invoke("get").arg(JExpr.lit(field.pos()))),
                                "ordinal"));
            }
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(containerSchema.getElementType().getFullName());

            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(
                                JExpr.invoke(
                                        JExpr.cast(enumClass, containerVariable.invoke("get").arg(counterVariable)),
                                        "getSchema"),
                                "getEnumOrdinal")
                                .arg(JExpr.invoke(
                                        JExpr.cast(enumClass, containerVariable.invoke("get").arg(counterVariable)),
                                        "toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(JExpr.cast(enumClass, containerVariable.invoke("get").arg(counterVariable)),
                                "ordinal"));
            }
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            final JClass enumClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(containerSchema.getValueType().getFullName());
            body.invoke(JExpr.direct(ENCODER), "writeString").arg(keyVariable);

            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(
                                JExpr.invoke(JExpr.cast(enumClass, containerVariable.invoke("get").arg(keyVariable)),
                                        "getSchema"),
                                "getEnumOrdinal")
                                .arg(JExpr.invoke(
                                        JExpr.cast(enumClass, containerVariable.invoke("get").arg(keyVariable)),
                                        "toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(
                        JExpr.invoke(JExpr.cast(enumClass, containerVariable.invoke("get").arg(keyVariable)),
                                "ordinal"));
            }
        } else if (Schema.Type.ENUM.equals(containerSchema.getType())) {
            if (useGenericTypes) {
                body.invoke(JExpr.direct(ENCODER), "writeEnum")
                        .arg(JExpr.invoke(containerVariable.invoke("getSchema"), "getEnumOrdinal")
                                .arg(containerVariable.invoke("toString")));
            } else {
                body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(containerVariable.invoke("ordinal"));
            }
        }
    }

    private void processPrimitive(JVar containerVariable, JVar keyVariable, final Schema containerSchema,
            final Schema primitiveSchema, JBlock body) {
        processPrimitive(containerVariable, keyVariable, null, containerSchema, primitiveSchema, null, body);
    }

    private void processPrimitive(JVar containerVariable, final Schema containerSchema, final Schema primitiveSchema,
            JVar counterVariable, JBlock body) {
        processPrimitive(containerVariable, null, counterVariable, containerSchema, primitiveSchema, null, body);
    }

    private void processPrimitive(JVar containerVariable, final Schema containerSchema, final Schema primitiveSchema,
            final Schema.Field field, JBlock body) {
        processPrimitive(containerVariable, null, null, containerSchema, primitiveSchema, field, body);
    }

    private void processPrimitive(JVar containerVariable, final Schema primitiveSchema, JBlock body) {
        processPrimitive(containerVariable, null, null, primitiveSchema, primitiveSchema, null, body);
    }

    private void processPrimitive(JVar containerVariable, JVar keyVariable, JVar counterVariable,
            final Schema containerSchema, final Schema primitiveSchema, final Schema.Field field, JBlock body) {
        String writeFunction = null;
        Class<?> castType = null;
        if (Schema.Type.BOOLEAN.equals(primitiveSchema.getType())) {
            writeFunction = "writeBoolean";
            castType = Boolean.class;
        } else if (Schema.Type.INT.equals(primitiveSchema.getType())) {
            writeFunction = "writeInt";
            castType = Integer.class;
        } else if (Schema.Type.LONG.equals(primitiveSchema.getType())) {
            writeFunction = "writeLong";
            castType = Long.class;
        } else if (Schema.Type.STRING.equals(primitiveSchema.getType())) {
            writeFunction = "writeString";
            castType = String.class;
        } else if (Schema.Type.DOUBLE.equals(primitiveSchema.getType())) {
            writeFunction = "writeDouble";
            castType = Double.class;
        } else if (Schema.Type.FLOAT.equals(primitiveSchema.getType())) {
            writeFunction = "writeFloat";
            castType = Float.class;
        } else if (Schema.Type.BYTES.equals(primitiveSchema.getType())) {
            writeFunction = "writeBytes";
            castType = ByteBuffer.class;
        }

        if (writeFunction == null) {
            throw new FastSerializerGeneratorException(
                    "Unsupported primitive schema of type: " + primitiveSchema.getType());
        }

        if (Schema.Type.RECORD.equals(containerSchema.getType())) {
            body.invoke(JExpr.direct(ENCODER), writeFunction).arg(
                    JExpr.cast(codeModel.ref(castType), containerVariable.invoke("get").arg(JExpr.lit(field.pos()))));
        } else if (Schema.Type.ARRAY.equals(containerSchema.getType())) {
            body.invoke(JExpr.direct(ENCODER), writeFunction)
                    .arg(JExpr.cast(codeModel.ref(castType), containerVariable.invoke("get").arg(counterVariable)));
        } else if (Schema.Type.MAP.equals(containerSchema.getType())) {
            body.invoke(JExpr.direct(ENCODER), "writeString").arg(keyVariable);
            body.invoke(JExpr.direct(ENCODER), writeFunction)
                    .arg(JExpr.cast(codeModel.ref(castType), containerVariable.invoke("get").arg(keyVariable)));
        } else {
            body.invoke(JExpr.direct(ENCODER), writeFunction)
                    .arg(JExpr.cast(codeModel.ref(castType), containerVariable));
        }
    }

    private JVar declareContainerVariableForSchemaInBlock(final String name, final Schema schema, JBlock block) {
        if (Schema.Type.ARRAY.equals(schema.getType())) {
            return block.decl(codeModel.ref(List.class).narrow(classFromArraySchemaElementType(schema)),
                    getVariableName(name), JExpr._null());
        } else if (Schema.Type.MAP.equals(schema.getType())) {
            return block
                    .decl(codeModel.ref(Map.class).narrow(codeModel.ref(String.class),
                            classFromMapSchemaElementType(schema)), getVariableName(name), JExpr._null());
        } else if (Schema.Type.RECORD.equals(schema.getType())) {
            return block.decl(
                    (useGenericTypes ? codeModel.ref(GenericData.Record.class) : codeModel.ref(schema.getFullName())),
                    getVariableName(name), JExpr._null());

        }

        return null;
    }

    private JClass classFromArraySchemaElementType(final Schema schema) {
        if (!Schema.Type.ARRAY.equals(schema.getType())) {
            throw new FastSerializerGeneratorException("Array schema was expected, instead got:"
                    + schema.getType().getName());
        }

        Schema.Type elementType = schema.getElementType().getType();

        if (Schema.Type.RECORD.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Record.class)
                    : codeModel.ref(schema.getElementType().getFullName());
        } else if (Schema.Type.ARRAY.equals(elementType)) {
            return codeModel.ref(List.class).narrow(
                    classFromArraySchemaElementType(schema.getElementType()));
        } else if (Schema.Type.MAP.equals(elementType)) {
            return codeModel.ref(Map.class).narrow(String.class)
                    .narrow(classFromMapSchemaElementType(schema.getElementType()));
        } else if (Schema.Type.ENUM.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(schema.getElementType().getFullName());
        } else if (Schema.Type.FIXED.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                : codeModel.ref(schema.getElementType().getFullName());
        } else if (Schema.Type.UNION.equals(elementType)) {
            return classFromUnionSchemaElementType(schema.getElementType());
        }

        try {
            String primitiveClassName;
            switch (schema.getElementType().getName()) {
                case "int":
                    primitiveClassName = "java.lang.Integer";
                    break;
                case "bytes":
                    primitiveClassName = "java.nio.ByteBuffer";
                    break;
                default:
                    primitiveClassName = "java.lang." + StringUtils.capitalize(StringUtils.lowerCase(schema
                        .getElementType().getName()));
            }
            return codeModel.ref(Class.forName(primitiveClassName));
        } catch (ReflectiveOperationException e) {
            throw new FastSerializerGeneratorException("Unknown type: " + schema
                .getElementType().getName(), e);
        }
    }

    private JClass classFromMapSchemaElementType(final Schema schema) {
        if (!schema.getType().equals(Schema.Type.MAP)) {
            throw new FastSerializerGeneratorException("Map schema was expected, instead got:"
                    + schema.getType().getName());
        }

        Schema.Type elementType = schema.getValueType().getType();

        if (Schema.Type.RECORD.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Record.class)
                    : codeModel.ref(schema.getValueType().getFullName());
        } else if (Schema.Type.ARRAY.equals(elementType)) {
            return codeModel.ref(List.class).narrow(classFromArraySchemaElementType(schema.getValueType()));
        } else if (Schema.Type.MAP.equals(elementType)) {
            return codeModel.ref(Map.class).narrow(String.class)
                    .narrow(classFromArraySchemaElementType(schema.getValueType()));
        } else if (Schema.Type.ENUM.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(schema.getValueType().getFullName());
        } else if (Schema.Type.FIXED.equals(elementType)) {
            return useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(schema.getValueType().getFullName());
        } else if (Schema.Type.UNION.equals(elementType)) {
            return classFromUnionSchemaElementType(schema.getValueType());
        }

        try {
            String primitiveClassName;
            switch (schema.getValueType().getName()) {
                case "int":
                    primitiveClassName = "java.lang.Integer";
                    break;
                case "bytes":
                    primitiveClassName = "java.nio.ByteBuffer";
                    break;
                default:
                    primitiveClassName = "java.lang." + StringUtils.capitalize(StringUtils.lowerCase(schema
                        .getValueType().getName()));
            }
            return codeModel.ref(Class.forName(primitiveClassName));
        } catch (ReflectiveOperationException e) {
            throw new FastSerializerGeneratorException("Unknown type: " + schema
                    .getValueType().getName(), e);
        }
    }

    private JClass classFromUnionSchemaElementType(final Schema schema) {
        if (!Schema.Type.UNION.equals(schema.getType())) {
            throw new FastSerializerGeneratorException("Union schema was expected, instead got:"
                    + schema.getType().getName());
        }

        if (schema.getTypes().size() > 2) {
            return codeModel.ref(Object.class);
        }

        Schema unionSchema = null;
        if (schema.getTypes().size() == 2) {
            if (Schema.Type.NULL.equals(schema.getTypes().get(0).getType())) {
                unionSchema = schema.getTypes().get(1);
            } else if (Schema.Type.NULL.equals(schema.getTypes().get(1).getType())) {
                unionSchema = schema.getTypes().get(0);
            } else {
                return codeModel.ref(Object.class);
            }
        }

        if (unionSchema != null) {
            if (Schema.Type.RECORD.equals(unionSchema.getType())) {
                return useGenericTypes ? codeModel.ref(GenericData.Record.class)
                        : codeModel.ref(unionSchema.getFullName());
            } else if (Schema.Type.ARRAY.equals(unionSchema.getType())) {
                return codeModel.ref(List.class).narrow(classFromArraySchemaElementType(unionSchema));
            } else if (Schema.Type.MAP.equals(unionSchema.getType())) {
                return codeModel.ref(Map.class).narrow(String.class)
                        .narrow(classFromArraySchemaElementType(unionSchema));
            } else if (Schema.Type.ENUM.equals(unionSchema.getType())) {
                return useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                        : codeModel.ref(unionSchema.getFullName());
            } else if (Schema.Type.FIXED.equals(unionSchema.getType())) {
                return useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                        : codeModel.ref(unionSchema.getFullName());
            }

            try {
                String primitiveClassName;
                switch (unionSchema.getName()) {
                    case "int":
                        primitiveClassName = "java.lang.Integer";
                        break;
                    case "bytes":
                        primitiveClassName = "java.nio.ByteBuffer";
                        break;
                    default:
                        primitiveClassName = "java.lang." + StringUtils.capitalize(StringUtils.lowerCase(unionSchema.getName()));
                }
                return codeModel.ref(Class.forName(primitiveClassName));
            } catch (ReflectiveOperationException e) {
                throw new FastSerializerGeneratorException("unknown type: " + unionSchema.getName(), e);
            }
        } else {
            throw new FastSerializerGeneratorException("Could not determine union element schema");
        }
    }

    private JClass classFromPrimitiveSchema(final Schema schema) {
        JClass primitiveClazz = null;
        switch (schema.getType()) {
        case BOOLEAN:
            primitiveClazz = codeModel.ref(Boolean.class);
            break;
        case DOUBLE:
            primitiveClazz = codeModel.ref(Double.class);
            break;
        case FLOAT:
            primitiveClazz = codeModel.ref(Float.class);
            break;
        case INT:
            primitiveClazz = codeModel.ref(Integer.class);
            break;
        case LONG:
            primitiveClazz = codeModel.ref(Long.class);
            break;
        case STRING:
            primitiveClazz = codeModel.ref(String.class);
            break;
        case BYTES:
            primitiveClazz = codeModel.ref(ByteBuffer.class);
            break;
        }

        return primitiveClazz;
    }

    private boolean doesNotContainMethod(final Schema schema) {
        return Schema.Type.RECORD.equals(schema.getType())
                && !serializeMethodMap.containsKey(schema.getFullName());
    }

    private JMethod getMethod(final Schema schema) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (!doesNotContainMethod(schema)) {
                return serializeMethodMap.get(schema.getFullName());
            }
            throw new FastSerializerGeneratorException("No method for schema: " + schema.getFullName());
        }
        throw new FastSerializerGeneratorException("No method for schema type: " + schema.getType());
    }

    private JMethod createMethod(final Schema schema) {
        if (Schema.Type.RECORD.equals(schema.getType())) {
            if (doesNotContainMethod(schema)) {
                JMethod method = serializerClass.method(JMod.PUBLIC, codeModel.VOID,
                        "serialize" + schema.getName() + nextRandomInt());
                method._throws(IOException.class);
                method.param(
                        useGenericTypes ? codeModel.ref(GenericData.Record.class) : codeModel.ref(schema.getFullName()),
                        "data");
                method.param(Encoder.class, ENCODER);

                method.annotate(SuppressWarnings.class).param("value", "unchecked");

                serializeMethodMap.put(schema.getFullName(), method);

                return method;
            } else {
                throw new FastSerializerGeneratorException("Method already exists for: " + schema.getFullName());
            }
        }
        throw new FastSerializerGeneratorException("No method for schema type: " + schema.getType());
    }

}
