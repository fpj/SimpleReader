package io.pravega.avro;

import io.pravega.client.stream.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class AvroSampleSerializer implements Serializer<Sample>, Serializable {
    static final Logger log = LoggerFactory.getLogger(AvroSampleSerializer.class);

    @Override
    public ByteBuffer serialize(Sample sample) {
        try {
            return sample.toByteBuffer();
        } catch (IOException e) {
            log.error("Error while serializing Avro object", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Sample deserialize(ByteBuffer byteBuffer) {
        try {
            return Sample.fromByteBuffer(byteBuffer);
        } catch (IOException e) {
            log.error("Error while deserializing Avro object", e);
            throw new RuntimeException(e);
        }
    }
}
