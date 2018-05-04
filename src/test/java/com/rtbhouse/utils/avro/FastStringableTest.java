package com.rtbhouse.utils.avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.specificDataAsDecoder;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.specificDataFromDecoder;

import com.rtbhouse.utils.generated.avro.StringableRecord;

public class FastStringableTest {

    private File tempDir;
    private ClassLoader classLoader;

    @Before
    public void prepare() throws Exception {
        Path tempPath = Files.createTempDirectory("generated");
        tempDir = tempPath.toFile();

        classLoader = URLClassLoader.newInstance(new URL[] { tempDir.toURI().toURL() },
                FastDeserializerDefaultsTest.class.getClassLoader());
    }

    @Test
    public void serializeStringableFields() throws URISyntaxException, MalformedURLException {

        //given
        BigInteger exampleBigInteger = new BigInteger(String.valueOf(Long.MAX_VALUE));
        exampleBigInteger = exampleBigInteger.pow(16);
        BigDecimal exampleBigDecimal = new BigDecimal(Double.MIN_VALUE);
        exampleBigDecimal = exampleBigDecimal.pow(16);
        File exampleFile = new File("/tmp/test");
        URI exampleURI = new URI("urn:ISSN:1522-3611");
        URL exampleURL = new URL("http://www.example.com");

        StringableRecord.Builder recordBuilder = StringableRecord.newBuilder();
        recordBuilder.setBigdecimal(exampleBigDecimal);
        recordBuilder.setBiginteger(exampleBigInteger);
        recordBuilder.setFile(exampleFile);
        recordBuilder.setUri(exampleURI);
        recordBuilder.setUrl(exampleURL);
        recordBuilder.setUrlArray(Collections.singletonList(exampleURL));
        recordBuilder.setUrlMap(Collections.singletonMap(exampleURL, exampleBigInteger));
        StringableRecord record = recordBuilder.build();

        //when
        StringableRecord afterDecoding = specificDataFromDecoder(StringableRecord.getClassSchema(), writeWithFastAvro(record, StringableRecord.getClassSchema()));

        //then
        Assert.assertEquals(exampleBigDecimal, afterDecoding.getBigdecimal());
        Assert.assertEquals(exampleBigInteger, afterDecoding.getBiginteger());
        Assert.assertEquals(exampleFile, afterDecoding.getFile());
        Assert.assertEquals(Collections.singletonList(exampleURL), afterDecoding.getUrlArray());
        Assert.assertEquals(Collections.singletonMap(exampleURL, exampleBigInteger), afterDecoding.getUrlMap());

    }

    @Test
    public void deserializeStringableFields() throws URISyntaxException, MalformedURLException {

        //given
        BigInteger exampleBigInteger = new BigInteger(String.valueOf(Long.MAX_VALUE));
        exampleBigInteger = exampleBigInteger.pow(16);
        BigDecimal exampleBigDecimal = new BigDecimal(Double.MIN_VALUE);
        exampleBigDecimal = exampleBigDecimal.pow(16);
        File exampleFile = new File("/tmp/test");
        URI exampleURI = new URI("urn:ISSN:1522-3611");
        URL exampleURL = new URL("http://www.example.com");

        StringableRecord.Builder recordBuilder = StringableRecord.newBuilder();
        recordBuilder.setBigdecimal(exampleBigDecimal);
        recordBuilder.setBiginteger(exampleBigInteger);
        recordBuilder.setFile(exampleFile);
        recordBuilder.setUri(exampleURI);
        recordBuilder.setUrl(exampleURL);
        recordBuilder.setUrlArray(Collections.singletonList(exampleURL));
        recordBuilder.setUrlMap(Collections.singletonMap(exampleURL, exampleBigInteger));
        StringableRecord record = recordBuilder.build();

        //when
        StringableRecord afterDecoding = readWithFastAvro(StringableRecord.getClassSchema(), StringableRecord.getClassSchema(), specificDataAsDecoder(record));

        //then
        Assert.assertEquals(exampleBigDecimal, afterDecoding.getBigdecimal());
        Assert.assertEquals(exampleBigInteger, afterDecoding.getBiginteger());
        Assert.assertEquals(exampleFile, afterDecoding.getFile());
        Assert.assertEquals(Collections.singletonList(exampleURL), afterDecoding.getUrlArray());
        Assert.assertEquals(Collections.singletonMap(exampleURL, exampleBigInteger), afterDecoding.getUrlMap());

    }

    public <T> Decoder writeWithFastAvro(T data, Schema schema) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(baos, null);

        try {
            FastSpecificSerializerGenerator<T> fastSpecificSerializerGenerator = new FastSpecificSerializerGenerator<>(
                    schema, tempDir, classLoader, null);
            FastSerializer<T> fastSerializer = fastSpecificSerializerGenerator.generateSerializer();
            fastSerializer.serialize(data, binaryEncoder);
            binaryEncoder.flush();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
    }

    public <T> T readWithFastAvro(Schema writerSchema, Schema readerSchema, Decoder decoder) {
        FastDeserializer<T> deserializer = new FastSpecificDeserializerGenerator<T>(writerSchema,
            readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
