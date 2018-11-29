# avro-fastserde

**avro-fastserde** is an alternative approach to [Apache Avro](http://avro.apache.org/) serialization and deserialization. It generates dedicated code responsible for handling serialization and deserialization, which achieves better performance results than native implementation. Learn more [here](http://techblog.rtbhouse.com/2017/04/18/fast-avro/).  

![build status](https://api.travis-ci.org/RTBHOUSE/avro-fastserde.svg?branch=master)

## Version

Current version is **1.0.3**

## Requirements

You need Java 8 to use this library.

## Installation

Releases are distributed on Maven central:

```xml
<dependency>
    <groupId>com.rtbhouse</groupId>
    <artifactId>avro-fastserde</artifactId>
    <version>1.0.3</version>
</dependency>
```

## Usage

Just use **avro-fastserde** `DatumReader` and `DatumWriter` interface implementation:

```java
import com.rtbhouse.utils.avro.FastGenericDatumReader;
import com.rtbhouse.utils.avro.FastGenericDatumWriter;
import com.rtbhouse.utils.avro.FastSpecificDatumReader;
import com.rtbhouse.utils.avro.FastSpecificDatumWriter;

...

FastGenericDatumReader<GenericData.Record> fastGenericDatumReader = new FastGenericDatumReader<>(writerSchema, readerSchema);
fastGenericDatumReader.read(null, binaryDecoder);

FastGenericDatumWriter<GenericData.Record> fastGenericDatumWriter = new FastGenericDatumWriter<>(schema);
fastGenericDatumWriter.read(data, binaryEncoder);

FastSpecificDatumReader<T> fastSpecificDatumReader = new FastSpecificDatumReader<>(writerSchema, readerSchema);
fastSpecificDatumReader.read(null, binaryDecoder);

FastSpecificDatumWriter<T> fastSpecificDatumWriter = new FastSpecificDatumWriter<>(schema);
fastSpecificDatumWriter.write(data, binaryEncoder);
```

You can alter class generation behaviour via system properties:
```java
 // Set compilation classpath
 System.setProperty(FastSerdeCache.CLASSPATH, compileClasspath);
 
 // Set generated classes directory
 System.setProperty(FastSerdeCache.GENERATED_CLASSES_DIR, generatedClassesDir);
```

Or `FastSerdeCache` class:

```java

import com.rtbhouse.utils.avro.FastGenericDatumReader;
import com.rtbhouse.utils.avro.FastSerdeCache;

...

FastSerdeCache cache = new FastSerdeCache(compileClassPath);
FastGenericDatumReader<GenericData.Record> fastGenericDatumReader = new FastGenericDatumReader<>(writerSchema, readerSchema, cache);
```

## Limitations

- no support for `reuse` parameter in `DatumReader` interface.
- no support for `SchemaConstructable` marker interface for specific Avro records.
- `FastSpecificDatumReader` will not read data into `GenericRecord` if the specific classes are not available but will result in compilation failure and fall back to default `SpecificDatumReader` implementation.
