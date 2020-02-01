package com.rtbhouse.utils.avro;

import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.deserializeSpecific;
import static com.rtbhouse.utils.avro.FastSerdeTestsSupport.serializeSpecific;

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

import com.rtbhouse.utils.generated.avro.AnotherSubRecord;
import com.rtbhouse.utils.generated.avro.StringableRecord;
import com.rtbhouse.utils.generated.avro.StringableSubRecord;

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
    public void shouldSerializeStringableFields() throws URISyntaxException, MalformedURLException {

        // given
        BigInteger exampleBigInteger = new BigInteger(String.valueOf(Long.MAX_VALUE)).pow(16);
        BigDecimal exampleBigDecimal = new BigDecimal(Double.MIN_VALUE).pow(16);
        File exampleFile = new File("/tmp/test");
        URI exampleURI = new URI("urn:ISSN:1522-3611");
        URL exampleURL = new URL("http://www.example.com");

        StringableRecord record = generateRecord(exampleURL, exampleURI, exampleFile, exampleBigDecimal,
                exampleBigInteger);

        // when
        StringableRecord afterDecoding = deserializeSpecific(StringableRecord.getClassSchema(),
                serializeSpecificFast(record, StringableRecord.getClassSchema()));

        // then
        Assert.assertEquals(exampleBigDecimal, afterDecoding.getBigdecimal());
        Assert.assertEquals(exampleBigInteger, afterDecoding.getBiginteger());
        Assert.assertEquals(exampleFile, afterDecoding.getFile());
        Assert.assertEquals(Collections.singletonList(exampleURL), afterDecoding.getUrlArray());
        Assert.assertEquals(Collections.singletonMap(exampleURL, exampleBigInteger), afterDecoding.getUrlMap());
        Assert.assertNotNull(afterDecoding.getSubRecord());
        Assert.assertEquals(exampleURI, afterDecoding.getSubRecord().getUriField());
        Assert.assertNotNull(afterDecoding.getSubRecordWithSubRecord());
        Assert.assertNotNull(afterDecoding.getSubRecordWithSubRecord().getSubRecord());
        Assert.assertEquals(exampleURI, afterDecoding.getSubRecordWithSubRecord().getSubRecord().getUriField());
    }

    @Test
    public void shouldDeserializeStringableFields() throws URISyntaxException, MalformedURLException {

        // given
        BigInteger exampleBigInteger = new BigInteger(String.valueOf(Long.MAX_VALUE)).pow(16);
        BigDecimal exampleBigDecimal = new BigDecimal(Double.MIN_VALUE).pow(16);
        File exampleFile = new File("/tmp/test");
        URI exampleURI = new URI("urn:ISSN:1522-3611");
        URL exampleURL = new URL("http://www.example.com");

        StringableRecord record = generateRecord(exampleURL, exampleURI, exampleFile, exampleBigDecimal,
                exampleBigInteger);

        // when
        StringableRecord afterDecoding = deserializeSpecificFast(StringableRecord.getClassSchema(),
                StringableRecord.getClassSchema(), serializeSpecific(record));

        // then
        Assert.assertEquals(exampleBigDecimal, afterDecoding.getBigdecimal());
        Assert.assertEquals(exampleBigInteger, afterDecoding.getBiginteger());
        Assert.assertEquals(exampleFile, afterDecoding.getFile());
        Assert.assertEquals(Collections.singletonList(exampleURL), afterDecoding.getUrlArray());
        Assert.assertEquals(Collections.singletonMap(exampleURL, exampleBigInteger), afterDecoding.getUrlMap());
        Assert.assertNotNull(afterDecoding.getSubRecord());
        Assert.assertEquals(exampleURI, afterDecoding.getSubRecord().getUriField());
        Assert.assertNotNull(afterDecoding.getSubRecordWithSubRecord());
        Assert.assertNotNull(afterDecoding.getSubRecordWithSubRecord().getSubRecord());
        Assert.assertEquals(exampleURI, afterDecoding.getSubRecordWithSubRecord().getSubRecord().getUriField());
    }

    private StringableRecord generateRecord(URL exampleURL, URI exampleURI, File exampleFile,
            BigDecimal exampleBigDecimal, BigInteger exampleBigInteger) {

        StringableSubRecord subrecord = new StringableSubRecord();
        subrecord.put(0, exampleURI);
        AnotherSubRecord anotherSubRecord = new AnotherSubRecord();
        anotherSubRecord.put(0, subrecord);

        StringableRecord.Builder recordBuilder = StringableRecord.newBuilder();
        recordBuilder.setBigdecimal(exampleBigDecimal);
        recordBuilder.setBiginteger(exampleBigInteger);
        recordBuilder.setFile(exampleFile);
        recordBuilder.setUri(exampleURI);
        recordBuilder.setUrl(exampleURL);
        recordBuilder.setUrlArray(Collections.singletonList(exampleURL));
        recordBuilder.setUrlMap(Collections.singletonMap(exampleURL, exampleBigInteger));
        recordBuilder.setSubRecord(subrecord);
        recordBuilder.setSubRecordWithSubRecord(anotherSubRecord);
        return recordBuilder.build();
    }

    private <T> Decoder serializeSpecificFast(T data, Schema schema) {
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

    private <T> T deserializeSpecificFast(Schema writerSchema, Schema readerSchema, Decoder decoder) {
        FastDeserializer<T> deserializer = new FastSpecificDeserializerGenerator<T>(writerSchema,
                readerSchema, tempDir, classLoader, null).generateDeserializer();

        try {
            return deserializer.deserialize(decoder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
