package io.confluent.sigmarules.flink.accumulator;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.sigmarules.models.AggregateValues;
import io.confluent.sigmarules.parsers.AggregateParser;
import org.apache.flink.api.common.accumulators.Accumulator;

import java.util.HashMap;
import java.util.Map;

public class SigmaWindowedStreamAccumulator implements Accumulator<JsonNode, HashMap<String, HashMap<String, Double>>> {

    private HashMap<String, HashMap<String, Double>> localValue = new HashMap<>();
    private final AggregateValues aggregateValues;
    private int count = 0;
    private Double sum = 0D;

    public SigmaWindowedStreamAccumulator(AggregateValues aggregateValues) {
        this.aggregateValues = aggregateValues;
    }

    @Override
    public void add(JsonNode value) {
        Map<String, Double> sigmaTimeFrameMap = localValue
                .computeIfAbsent(
                        value.get("sigmaGroupByKey").asText(),
                        k -> new HashMap<String, Double>()
                );

        String timeFrameKey = value.get("sigmaTimeFrame").asText();

        sigmaTimeFrameMap.computeIfAbsent(timeFrameKey, k -> 0D);

        switch (aggregateValues.getAggregateFunction()) {
            case AggregateParser.COUNT:
                sigmaTimeFrameMap
                        .put(
                                timeFrameKey,
                                sigmaTimeFrameMap.get(timeFrameKey) + 1D
                        );
                break;

            case AggregateParser.SUM:
                sigmaTimeFrameMap
                        .put(
                                timeFrameKey,
                                sigmaTimeFrameMap.get(
                                        timeFrameKey
                                ) + value.get(aggregateValues.getDistinctValue()).asDouble()
                        );
                break;

            case AggregateParser.AVG:
                this.count++;
                this.sum += value.get(aggregateValues.getDistinctValue()).asDouble();
                sigmaTimeFrameMap.put(timeFrameKey, this.sum / this.count);
                break;
            case AggregateParser.MIN:
                sigmaTimeFrameMap.put(timeFrameKey, Math.min(sigmaTimeFrameMap.get(timeFrameKey), value.get(aggregateValues.getDistinctValue()).asDouble()));
                break;
            case AggregateParser.MAX:
                sigmaTimeFrameMap.put(timeFrameKey, Math.max(sigmaTimeFrameMap.get(timeFrameKey), value.get(aggregateValues.getDistinctValue()).asDouble()));
                break;
            default:
                throw new UnsupportedOperationException("Sigma Aggregation Operation not supported");
        }
    }

    @Override
    public HashMap<String, HashMap<String, Double>> getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue = new HashMap<>();
    }

    @Override
    public void merge(Accumulator<JsonNode, HashMap<String, HashMap<String, Double>>> other) {

        if (!(other instanceof SigmaWindowedStreamAccumulator)) {
            throw new IllegalArgumentException(
                    "The merged accumulator must be SigmaWindowedStreamAccumulator.");
        }

        ((SigmaWindowedStreamAccumulator) other).sum = this.sum + ((SigmaWindowedStreamAccumulator) other).sum;
        ((SigmaWindowedStreamAccumulator) other).count = this.count + ((SigmaWindowedStreamAccumulator) other).count;

        for (String groupByKey : localValue.keySet()) {
            HashMap<String, Double> groupByValue = other.getLocalValue().computeIfAbsent(groupByKey, k -> new HashMap<String, Double>());
            for (String timeFrameKey : localValue.get(groupByKey).keySet()) {
                Double aggregate = groupByValue
                        .computeIfAbsent(timeFrameKey, k -> 0D);
                switch (aggregateValues.getAggregateFunction()) {
                    case AggregateParser.COUNT:
                    case AggregateParser.SUM:
                        other.getLocalValue().get(groupByKey).put(timeFrameKey, aggregate + localValue.get(groupByKey).get(timeFrameKey));
                        break;
                    case AggregateParser.AVG:
                        int count = ((SigmaWindowedStreamAccumulator) other).count + this.count;
                        double sum = this.sum + ((SigmaWindowedStreamAccumulator) other).sum;
                        Double avg = sum / count;
                        other.getLocalValue().get(groupByKey).put(timeFrameKey, avg);
                        break;
                    case AggregateParser.MIN:
                        other.getLocalValue().get(groupByKey).put(timeFrameKey, Math.min(aggregate, localValue.get(groupByKey).get(timeFrameKey)));
                        break;
                    case AggregateParser.MAX:
                        other.getLocalValue().get(groupByKey).put(timeFrameKey, Math.max(aggregate, localValue.get(groupByKey).get(timeFrameKey)));
                        break;
                    default:
                        throw new UnsupportedOperationException("Sigma Aggregation Operation not supported");
                }
            }
        }
    }

    @Override
    public Accumulator<JsonNode, HashMap<String, HashMap<String, Double>>> clone() {
        SigmaWindowedStreamAccumulator clone = new SigmaWindowedStreamAccumulator(this.aggregateValues);
        clone.localValue = new HashMap<>();
        clone.sum = sum;
        clone.count = count;

        for (String groupByKey : this.localValue.keySet()) {
            HashMap<String, Double> groupByValue = new HashMap<>();
            for (String timeFrameKey : this.localValue.get(groupByKey).keySet()) {
                groupByValue.put(timeFrameKey, this.localValue.get(groupByKey).get(timeFrameKey));
            }
            clone.localValue.put(groupByKey, groupByValue);
        }
        return clone;
    }
}
