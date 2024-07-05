package io.confluent.sigmarules.flink.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class JsonNodeKafkaRecordDeserializationSchema implements KafkaRecordDeserializationSchema<JsonNode> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final boolean includeMetadata;

    public JsonNodeKafkaRecordDeserializationSchema(boolean includeMetadata) {
        this.includeMetadata = includeMetadata;
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return new TypeInformation<JsonNode>() {
            @Override
            public boolean isBasicType() {
                return false;
            }

            @Override
            public boolean isTupleType() {
                return false;
            }

            @Override
            public int getArity() {
                return 0;
            }

            @Override
            public int getTotalFields() {
                return 0;
            }

            @Override
            public Class<JsonNode> getTypeClass() {
                return JsonNode.class;
            }

            @Override
            public boolean isKeyType() {
                return false;
            }

            @Override
            public TypeSerializer<JsonNode> createSerializer(ExecutionConfig config) {
                return new JsonNodeSerializer();
            }

            @Override
            public String toString() {
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    return objectMapper.writeValueAsString(this);
                } catch (JsonProcessingException e) {
                    return "{}";
                }
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == this) {
                    return true;
                }

                if (!(obj instanceof JsonNodeDeserializationSchema)) {
                    return false;
                }

                return true;
            }

            @Override
            public int hashCode() {
                return 764564738;
            }

            @Override
            public boolean canEqual(Object obj) {
                return equals(obj);
            }
        };
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<JsonNode> collector) throws IOException {
        ObjectNode node = this.objectMapper.createObjectNode();
        if (consumerRecord.key() != null) {
            node.set("key", (JsonNode) this.objectMapper.readValue((byte[]) consumerRecord.key(), JsonNode.class));
        }

        if (consumerRecord.value() != null) {
            node.set("value", (JsonNode) this.objectMapper.readValue((byte[]) consumerRecord.value(), JsonNode.class));
        }

        if (this.includeMetadata) {
            node.putObject("metadata").put("offset", consumerRecord.offset()).put("topic", consumerRecord.topic()).put("partition", consumerRecord.partition());
        }

    }
}
