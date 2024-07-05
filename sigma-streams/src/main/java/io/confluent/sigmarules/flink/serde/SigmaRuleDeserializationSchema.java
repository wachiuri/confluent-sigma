package io.confluent.sigmarules.flink.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.sigmarules.models.SigmaRule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.util.JacksonMapperFactory;

import java.io.IOException;

public class SigmaRuleDeserializationSchema implements DeserializationSchema<SigmaRule> {

    private ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = JacksonMapperFactory.createObjectMapper();
    }

    @Override
    public SigmaRule deserialize(byte[] message) throws IOException {
        return this.objectMapper.readValue(message, SigmaRule.class);
    }

    @Override
    public boolean isEndOfStream(SigmaRule nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SigmaRule> getProducedType() {
        return TypeExtractor.getForClass(SigmaRule.class);
    }
}
