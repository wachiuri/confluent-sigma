package io.confluent.sigmarules.flink.keyselector;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.sigmarules.models.AggregateValues;
import io.confluent.sigmarules.models.SigmaRule;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class SigmaKeySelector implements KeySelector<Tuple2<ObjectNode, SigmaRule>, String> {
    @Override
    public String getKey(Tuple2<ObjectNode, SigmaRule> value) throws Exception {
        AggregateValues aggregateValues = value.f1.getConditionsManager().getAggregateCondition()
                .getAggregateValues();

        String newKey = value.f1.getTitle();
        String groupBy = aggregateValues.getGroupBy();
        if (groupBy != null && !groupBy.isEmpty() && value.f0.get(groupBy) != null) {
            newKey = newKey + "-" + value.f0.get(aggregateValues.getGroupBy()).asText();
        }

        String distinctValue = aggregateValues.getDistinctValue();
        if (distinctValue != null && !distinctValue.isEmpty() &&
                value.f0.get(distinctValue) != null) {
            newKey = newKey + "-" + value.f0.get(distinctValue).asText();
        }

        String operation = aggregateValues.getOperation();
        if (operation != null && !operation.isEmpty()) {
            newKey = newKey + "-" + operation;
        }

        String operationValue = aggregateValues.getOperationValue();
        if (operationValue != null && !operationValue.isEmpty()) {
            newKey = newKey + "-" + operationValue;
        }

        return newKey;
    }
}
