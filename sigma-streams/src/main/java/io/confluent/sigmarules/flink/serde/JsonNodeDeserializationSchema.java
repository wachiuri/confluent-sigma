package io.confluent.sigmarules.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.util.JacksonMapperFactory;

import java.io.IOException;

public class JsonNodeDeserializationSchema implements DeserializationSchema<ObjectNode> {

    private ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        this.mapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public ObjectNode deserialize(byte[] message) throws IOException {

        return (ObjectNode) this.mapper.readValue( message, ObjectNode.class);
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

    @Override
    public TypeInformation<ObjectNode> getProducedType() {
        return TypeExtractor.getForClass(ObjectNode.class);
    }
}
