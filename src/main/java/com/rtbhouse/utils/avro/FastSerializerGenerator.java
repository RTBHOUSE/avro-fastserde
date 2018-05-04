package com.rtbhouse.utils.avro;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.Encoder;

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

    private final boolean useGenericTypes;
    private final Map<String, JMethod> serializeMethodMap = new HashMap<>();
    private final SchemaMapper schemaMapper;

    public FastSerializerGenerator(boolean useGenericTypes, Schema schema, File destination, ClassLoader classLoader,
            String compileClassPath) {
        super(schema, destination, classLoader, compileClassPath);
        this.useGenericTypes = useGenericTypes;
        this.schemaMapper = new SchemaMapper(codeModel, useGenericTypes);
    }

    @Override
    public FastSerializer<T> generateSerializer() {
        final String className = getClassName(schema, useGenericTypes ? "Generic" : "Specific");
        final JPackage classPackage = codeModel._package(GENERATED_PACKAGE_NAME);

        try {
            serializerClass = classPackage._class(className);

            final JMethod serializeMethod = serializerClass.method(JMod.PUBLIC, void.class, "serialize");
            final JVar serializeMethodParam;

            JClass outputClass = schemaMapper.classFromSchema(schema);
            serializerClass._implements(codeModel.ref(FastSerializer.class).narrow(outputClass));
            serializeMethodParam = serializeMethod.param(outputClass, "data");

            switch (schema.getType()) {
            case RECORD:
                processRecord(schema, serializeMethodParam, serializeMethod.body());
                break;
            case ARRAY:
                processArray(schema, serializeMethodParam, serializeMethod.body());
                break;
            case MAP:
                processMap(schema, serializeMethodParam, serializeMethod.body());
                break;
            default:
                throw new FastSerializerGeneratorException("Unsupported input schema type: " + schema.getType());
            }

            serializeMethod.annotate(SuppressWarnings.class).param("value", "unchecked");
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

    private void processComplexType(Schema schema, JVar variable, JBlock body) {
        switch (schema.getType()) {
        case RECORD:
            processRecord(schema, variable, body);
            break;
        case ARRAY:
            processArray(schema, variable, body);
            break;
        case UNION:
            processUnion(schema, variable, body);
            break;
        case MAP:
            processMap(schema, variable, body);
            break;
        default:
            throw new FastSerializerGeneratorException("Not a complex schema type: " + schema.getType());
        }

    }

    private void processSimpleType(Schema schema, JExpression valueExpression, JBlock body) {
        switch (schema.getType()) {
        case ENUM:
            processEnum(schema, valueExpression, body);
            break;
        case FIXED:
            processFixed(schema, valueExpression, body);
            break;
        default:
            processPrimitive(schema, valueExpression, body);
        }
    }

    private void processRecord(final Schema recordSchema, JVar recordVar, final JBlock containerBody) {
        if (!doesNotContainMethod(recordSchema)) {
            containerBody.invoke(getMethod(recordSchema)).arg(recordVar).arg(JExpr.direct(ENCODER));
            return;
        }
        JMethod method = createMethod(recordSchema);
        containerBody.invoke(getMethod(recordSchema)).arg(recordVar).arg(JExpr.direct(ENCODER));

        JBlock body = method.body();
        recordVar = method.listParams()[0];

        for (Schema.Field field : recordSchema.getFields()) {
            Schema fieldSchema = field.schema();
            if (SchemaMapper.isComplexType(fieldSchema)) {
                JClass fieldClass = schemaMapper.classFromSchema(fieldSchema);
                JVar containerVar = declareContainerVariableForSchemaInBlock(field.name(), fieldSchema, body);
                JExpression valueExpression = JExpr.invoke(recordVar, "get").arg(JExpr.lit(field.pos()));
                containerVar.init(JExpr.cast(fieldClass, valueExpression));

                processComplexType(fieldSchema, containerVar, body);
            } else {
                processSimpleType(fieldSchema, recordVar.invoke("get").arg(JExpr.lit(field.pos())), body);
            }

        }
    }

    private void processArray(final Schema arraySchema, JVar arrayVar, JBlock body) {
        final JClass arrayClass = schemaMapper.classFromSchema(arraySchema);
        body.invoke(JExpr.direct(ENCODER), "writeArrayStart");

        final JExpression emptyArrayCondition = arrayVar.eq(JExpr._null())
                .cor(JExpr.invoke(JExpr.cast(arrayClass, arrayVar), "size").eq(JExpr.lit(0)));

        final JConditional emptyArrayIf = body._if(emptyArrayCondition);
        final JBlock emptyArrayBlock = emptyArrayIf._then();
        emptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
        emptyArrayBlock.invoke(JExpr.direct(ENCODER), "writeArrayEnd");

        final JBlock nonEmptyArrayBlock = emptyArrayIf._else();
        nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "setItemCount")
                .arg(JExpr.invoke(JExpr.cast(arrayClass, arrayVar), "size"));
        final JForLoop forLoop = nonEmptyArrayBlock._for();
        final JVar counter = forLoop.init(codeModel.INT, getVariableName("counter"), JExpr.lit(0));
        forLoop.test(counter.lt(JExpr.invoke(JExpr.cast(arrayClass, arrayVar), "size")));
        forLoop.update(counter.incr());
        final JBlock forBody = forLoop.body();
        forBody.invoke(JExpr.direct(ENCODER), "startItem");

        final Schema elementSchema = arraySchema.getElementType();
        if (SchemaMapper.isComplexType(elementSchema)) {
            JVar containerVar = declareContainerVariableForSchemaInBlock(getVariableName(elementSchema.getName()),
                    elementSchema, forBody);
            forBody.assign(containerVar, JExpr.invoke(JExpr.cast(arrayClass, arrayVar), "get").arg(counter));
            processComplexType(elementSchema, containerVar, forBody);
        } else {
            processSimpleType(elementSchema, arrayVar.invoke("get").arg(counter), forBody);
        }
        nonEmptyArrayBlock.invoke(JExpr.direct(ENCODER), "writeArrayEnd");
    }

    private void processMap(final Schema mapSchema, JVar mapVar, JBlock body) {

        final JClass mapClass = schemaMapper.classFromSchema(mapSchema);

        JClass keyClass = schemaMapper.keyClassFromMapSchema(mapSchema);

        body.invoke(JExpr.direct(ENCODER), "writeMapStart");

        final JExpression emptyMapCondition = mapVar.eq(JExpr._null())
                .cor(JExpr.invoke(JExpr.cast(mapClass, mapVar), "size").eq(JExpr.lit(0)));
        final JConditional emptyMapIf = body._if(emptyMapCondition);
        final JBlock emptyMapBlock = emptyMapIf._then();
        emptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount").arg(JExpr.lit(0));
        emptyMapBlock.invoke(JExpr.direct(ENCODER), "writeMapEnd");

        final JBlock nonEmptyMapBlock = emptyMapIf._else();
        nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "setItemCount")
                .arg(JExpr.invoke(JExpr.cast(mapClass, mapVar), "size"));

        final JForEach mapKeysLoop = nonEmptyMapBlock.forEach(keyClass, getVariableName("key"),
                JExpr.invoke(JExpr.cast(mapClass, mapVar), "keySet"));

        final JBlock forBody = mapKeysLoop.body();
        forBody.invoke(JExpr.direct(ENCODER), "startItem");

        JVar keyStringVar;
        if (SchemaMapper.haveStringableKey(mapSchema)) {
            keyStringVar = forBody.decl(codeModel.ref(String.class), getVariableName("keyString"),
                    mapKeysLoop.var().invoke("toString"));
        } else {
            keyStringVar = mapKeysLoop.var();
        }

        final Schema valueSchema = mapSchema.getValueType();

        forBody.invoke(JExpr.direct(ENCODER), "writeString").arg(keyStringVar);

        JVar containerVar;
        if (SchemaMapper.isComplexType(valueSchema)) {
            containerVar = declareContainerVariableForSchemaInBlock(valueSchema.getName(), valueSchema, forBody);
            forBody.assign(containerVar, JExpr.invoke(JExpr.cast(mapClass, mapVar), "get").arg(mapKeysLoop.var()));

            processComplexType(valueSchema, containerVar, forBody);
        } else {
            processSimpleType(valueSchema, mapVar.invoke("get").arg(mapKeysLoop.var()), forBody);
        }
        nonEmptyMapBlock.invoke(JExpr.direct(ENCODER), "writeMapEnd");
    }

    private void processUnion(final Schema unionSchema, JVar unionVar, JBlock body) {

        JConditional ifBlock = null;
        for (Schema schemaOption : unionSchema.getTypes()) {
            // Special handling for null
            if (Schema.Type.NULL.equals(schemaOption.getType())) {
                JExpression condition = unionVar.eq(JExpr._null());
                ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
                JBlock thenBlock = ifBlock._then();
                thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                        .arg(JExpr.lit(unionSchema.getIndexNamed(schemaOption.getType().getName())));
                thenBlock.invoke(JExpr.direct(ENCODER), "writeNull");
                continue;
            }

            JClass optionClass = schemaMapper.classFromSchema(schemaOption);
            JClass rawOptionClass = schemaMapper.classFromSchema(schemaOption, true, true);
            JExpression condition = unionVar._instanceof(rawOptionClass);
            if (useGenericTypes && SchemaMapper.isNamedType(schemaOption)) {
                condition = condition.cand(JExpr.invoke(JExpr.lit(schemaOption.getFullName()), "equals")
                        .arg(JExpr.invoke(JExpr.cast(optionClass, unionVar), "getSchema").invoke("getFullName")));
            }
            ifBlock = ifBlock != null ? ifBlock._elseif(condition) : body._if(condition);
            JBlock thenBlock = ifBlock._then();
            thenBlock.invoke(JExpr.direct(ENCODER), "writeIndex")
                    .arg(JExpr.lit(unionSchema.getIndexNamed(schemaOption.getFullName())));
            JVar optionVar = thenBlock
                    .decl(optionClass, getVariableName(schemaOption.getName()), JExpr.cast(optionClass, unionVar));

            switch (schemaOption.getType()) {
            case UNION:
            case NULL:
                throw new FastSerializerGeneratorException("Incorrect union subschema processing: " + schemaOption);
            default:
                if (SchemaMapper.isComplexType(schemaOption)) {
                    processComplexType(schemaOption, optionVar, thenBlock);
                } else {
                    processSimpleType(schemaOption, optionVar, thenBlock);
                }
            }
        }
    }

    private void processFixed(Schema fixedSchema, JExpression fixedValueExpression, JBlock body) {
        JClass fixedClass = schemaMapper.classFromSchema(fixedSchema);
        body.invoke(JExpr.direct(ENCODER), "writeFixed")
                .arg(JExpr.invoke(JExpr.cast(fixedClass, fixedValueExpression), "bytes"));
    }

    private void processEnum(Schema enumSchema, JExpression enumValueExpression, JBlock body) {
        JClass enumClass = schemaMapper.classFromSchema(enumSchema);
        JExpression enumValueCasted = JExpr.cast(enumClass, enumValueExpression);
        JExpression valueToWrite;
        if (useGenericTypes) {
            valueToWrite = JExpr.invoke(enumValueCasted.invoke("getSchema"), "getEnumOrdinal")
                    .arg(enumValueCasted.invoke("toString"));
        } else {
            valueToWrite = enumValueCasted.invoke("ordinal");
        }

        body.invoke(JExpr.direct(ENCODER), "writeEnum").arg(valueToWrite);
    }

    private void processPrimitive(final Schema primitiveSchema, JExpression primitiveValueExpression, JBlock body) {
        String writeFunction;
        JClass primitiveClass = schemaMapper.classFromSchema(primitiveSchema);
        JExpression primitiveValueCasted = JExpr.cast(primitiveClass, primitiveValueExpression);
        switch (primitiveSchema.getType()) {
        case STRING:
            writeFunction = "writeString";
            if (SchemaMapper.isStringable(primitiveSchema)) {
                primitiveValueCasted = JExpr.cast(codeModel.ref(String.class), primitiveValueCasted.invoke("toString"));
            }
            break;
        case BYTES:
            writeFunction = "writeBytes";
            break;
        case INT:
            writeFunction = "writeInt";
            break;
        case LONG:
            writeFunction = "writeLong";
            break;
        case FLOAT:
            writeFunction = "writeFloat";
            break;
        case DOUBLE:
            writeFunction = "writeDouble";
            break;
        case BOOLEAN:
            writeFunction = "writeBoolean";
            break;
        default:
            throw new FastSerializerGeneratorException(
                    "Unsupported primitive schema of type: " + primitiveSchema.getType());
        }

        body.invoke(JExpr.direct(ENCODER), writeFunction).arg(primitiveValueCasted);
    }

    private JVar declareContainerVariableForSchemaInBlock(final String name, final Schema schema, JBlock block) {
        if (SchemaMapper.isComplexType(schema)) {
            JClass containerClass = schemaMapper.classFromSchema(schema, true);
            return block.decl(containerClass, getVariableName(name), JExpr._null());
        } else {
            throw new FastDeserializerGeneratorException("Incorrect container variable: " + schema.getType().getName());
        }
    }

    private boolean doesNotContainMethod(final Schema schema) {
        return Schema.Type.RECORD.equals(schema.getType()) && !serializeMethodMap.containsKey(schema.getFullName());
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
                method.param(schemaMapper.classFromSchema(schema), "data");
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
