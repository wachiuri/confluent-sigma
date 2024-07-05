package io.confluent.sigmarules.flink.serde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.sigmarules.utilities.JsonUtils;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class JsonNodeSerializer extends TypeSerializer<JsonNode> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<JsonNode> duplicate() {
        return this;
    }

    @Override
    public JsonNode createInstance() {
        return JsonUtils.toJsonNode("{}");
    }

    @Override
    public JsonNode copy(JsonNode from) {
        return from.deepCopy();
    }

    @Override
    public JsonNode copy(JsonNode from, JsonNode reuse) {
        return from.deepCopy();
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(JsonNode record, DataOutputView target) throws IOException {
        byte[] bytes = objectMapper.writeValueAsBytes(record);
        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public JsonNode deserialize(DataInputView source) throws IOException {
        int length = source.readInt();
        byte[] bytes = new byte[length];
        source.readFully(bytes);
        return objectMapper.readTree(bytes);
    }

    @Override
    public JsonNode deserialize(JsonNode reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        JsonNode record = deserialize(source);
        serialize(record, target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof JsonNodeSerializer;
    }

    @Override
    public int hashCode() {
        return JsonNodeSerializer.class.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<JsonNode> snapshotConfiguration() {
        return new GenericTypeSerializerSnapshot<JsonNode, JsonNodeSerializer>() {
            @Override
            protected Class<JsonNode> getTypeClass(JsonNodeSerializer serializer) {
                return JsonNode.class;
            }

            @Override
            protected Class<?> serializerClass() {
                return JsonNodeSerializer.class;
            }

            @Override
            protected TypeSerializer<JsonNode> createSerializer(Class typeClass) {
                return new JsonNodeSerializer();
            }
        };
    }
}
