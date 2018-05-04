package com.rtbhouse.utils.avro;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JInvocation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SchemaMapper {
    private final JCodeModel codeModel;
    private final boolean useGenericTypes;

    SchemaMapper(JCodeModel codeModel, boolean useGenericTypes) {
        this.codeModel = codeModel;
        this.useGenericTypes = useGenericTypes;
    }

    JClass keyClassFromMapSchema(Schema schema) {
        if (!Schema.Type.MAP.equals(schema.getType())) {
            throw new SchemaMapperException("Map schema was expected, instead got:"
                    + schema.getType().getName());
        }
        if (haveStringableKey(schema) && !useGenericTypes) {
            return codeModel.ref(schema.getProp("java-key-class"));
        } else {
            return codeModel.ref(String.class);
        }
    }

    private JClass valueClassFromMapSchema(Schema schema) {
        if (!Schema.Type.MAP.equals(schema.getType())) {
            throw new SchemaMapperException("Map schema was expected, instead got:"
                    + schema.getType().getName());
        }

        return classFromSchema(schema.getValueType());
    }

    private JClass elementClassFromArraySchema(Schema schema) {
        if (!Schema.Type.ARRAY.equals(schema.getType())) {
            throw new SchemaMapperException("Array schema was expected, instead got:"
                    + schema.getType().getName());
        }

        return classFromSchema(schema.getElementType());
    }

    private JClass classFromUnionSchema(final Schema schema) {
        if (!Schema.Type.UNION.equals(schema.getType())) {
            throw new SchemaMapperException("Union schema was expected, instead got:"
                    + schema.getType().getName());
        }

        if (schema.getTypes().size() == 1) {
            return classFromSchema(schema.getTypes().get(0));
        }

        if (schema.getTypes().size() == 2) {
            if (Schema.Type.NULL.equals(schema.getTypes().get(0).getType())) {
                return classFromSchema(schema.getTypes().get(1));
            } else if (Schema.Type.NULL.equals(schema.getTypes().get(1).getType())) {
                return classFromSchema(schema.getTypes().get(0));
            }
        }

        return codeModel.ref(Object.class);
    }

    JClass classFromSchema(Schema schema) {
        return classFromSchema(schema, true, false);
    }

    JClass classFromSchema(Schema schema, boolean abstractType) {
        return classFromSchema(schema, abstractType, false);
    }

    /* Note that settings abstractType and rawType are not passed subcalls */
    JClass classFromSchema(Schema schema, boolean abstractType, boolean rawType) {
        JClass outputClass;

        switch (schema.getType()) {

        case RECORD:
            outputClass = useGenericTypes ? codeModel.ref(GenericData.Record.class)
                    : codeModel.ref(schema.getFullName());
            break;

        case ARRAY:
            if (abstractType) {
                outputClass = codeModel.ref(List.class);
            } else {
                if (useGenericTypes) {
                    outputClass = codeModel.ref(GenericData.Array.class);
                } else {
                    outputClass = codeModel.ref(ArrayList.class);
                }
            }
            if (!rawType) {
                outputClass = outputClass.narrow(elementClassFromArraySchema(schema));
            }
            break;
        case MAP:
            if (!abstractType) {
                outputClass = codeModel.ref(HashMap.class);
            } else {
                outputClass = codeModel.ref(Map.class);
            }
            if (!rawType) {
                outputClass = outputClass.narrow(keyClassFromMapSchema(schema), valueClassFromMapSchema(schema));
            }
            break;
        case UNION:
            outputClass = classFromUnionSchema(schema);
            break;
        case ENUM:
            outputClass = useGenericTypes ? codeModel.ref(GenericData.EnumSymbol.class)
                    : codeModel.ref(schema.getFullName());
            break;
        case FIXED:
            outputClass = useGenericTypes ? codeModel.ref(GenericData.Fixed.class)
                    : codeModel.ref(schema.getFullName());
            break;
        case BOOLEAN:
            outputClass = codeModel.ref(Boolean.class);
            break;
        case DOUBLE:
            outputClass = codeModel.ref(Double.class);
            break;
        case FLOAT:
            outputClass = codeModel.ref(Float.class);
            break;
        case INT:
            outputClass = codeModel.ref(Integer.class);
            break;
        case LONG:
            outputClass = codeModel.ref(Long.class);
            break;
        case STRING:
            if (isStringable(schema) && !useGenericTypes) {
                outputClass = codeModel.ref(schema.getProp("java-class"));
            } else {
                outputClass = codeModel.ref(String.class);
            }
            break;
        case BYTES:
            outputClass = codeModel.ref(ByteBuffer.class);
            break;
        default:
            throw new SchemaMapperException("Incorrect request for class for " + schema.getType().getName() + " type!");
        }

        return outputClass;
    }

    public JExpression getEnumValueByName(Schema enumSchema, JExpression nameExpr, JInvocation getSchemaExpr) {
        if (useGenericTypes) {
            return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchemaExpr).arg(nameExpr);
        } else {
            return codeModel.ref(enumSchema.getFullName()).staticInvoke("valueOf").arg(nameExpr);
        }
    }

    public JExpression getEnumValueByIndex(Schema enumSchema, JExpression indexExpr, JInvocation getSchemaExpr) {
        if (useGenericTypes) {
            return JExpr._new(codeModel.ref(GenericData.EnumSymbol.class)).arg(getSchemaExpr)
                    .arg(getSchemaExpr.invoke("getEnumSymbols").invoke("get").arg(indexExpr));
        } else {
            return codeModel.ref(enumSchema.getFullName()).staticInvoke("values").component(indexExpr);
        }
    }

    public JExpression getFixedValue(Schema schema, JExpression fixedBytesExpr, JInvocation getSchemaExpr) {
        if (!useGenericTypes) {
            return JExpr._new(codeModel.ref(schema.getFullName())).arg(fixedBytesExpr);
        } else {
            return JExpr._new(codeModel.ref(GenericData.Fixed.class)).arg(getSchemaExpr).arg(fixedBytesExpr);
        }
    }

    public JExpression getStringableValue(Schema schema, JExpression stringExpr) {
        if (isStringable(schema)) {
            return JExpr._new(classFromSchema(schema)).arg(stringExpr);
        } else {
            return stringExpr;
        }
    }

    /* Complex type here means type that it have to handle other types inside itself. */
    public static boolean isComplexType(Schema schema) {
        switch (schema.getType()) {
        case MAP:
        case RECORD:
        case ARRAY:
        case UNION:
            return true;
        default:
            return false;
        }
    }

    public static boolean isNamedType(Schema schema) {
        switch (schema.getType()) {
        case RECORD:
        case ENUM:
        case FIXED:
            return true;
        default:
            return false;
        }
    }

    public static boolean isStringable(Schema schema) {
        if (!Schema.Type.STRING.equals(schema.getType())) {
            throw new SchemaMapperException("String schema expected!");
        }
        return schema.getProp("java-class") != null;
    }

    public static boolean haveStringableKey(Schema schema) {
        if (!Schema.Type.MAP.equals(schema.getType())) {
            throw new SchemaMapperException("String schema expected!");
        }
        return schema.getProp("java-key-class") != null;
    }
}
