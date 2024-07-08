package io.confluent.sigmarules.flink.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.util.JacksonMapperFactory;

import java.io.IOException;

public class JsonNodeDeserializationSchema implements DeserializationSchema<ObjectNode>, SerializationSchema<ObjectNode> {

    private static final long serialVersionUID = 874563476L;

    private ObjectMapper mapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        this.mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public ObjectNode deserialize(byte[] message) throws IOException {
        return this.mapper.readValue( message, ObjectNode.class);
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }

    @Override
    public byte[] serialize(ObjectNode objectNode) {
        try {
            return this.mapper.writeValueAsBytes(objectNode);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
