package io.confluent.sigmarules.flink.accumulator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.parsers.AggregateParser;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SigmaWindowedStreamAccumulator implements Accumulator<Tuple2<ObjectNode, SigmaRule>, Tuple2<SigmaRule, SigmaAggregate>> {

    private static final Logger logger = LoggerFactory.getLogger(SigmaWindowedStreamAccumulator.class);

    private Tuple2<SigmaRule, SigmaAggregate> localValue = Tuple2.of(null, new SigmaAggregate());
    private int count = 0;
    private Double sum = 0D;

    private SigmaAggregateEntry computeIfAbsent(Long start, Long end) {

        for (SigmaAggregateEntry sigmaAggregateEntry : localValue.f1.getAggregates()) {
            if (
                    sigmaAggregateEntry.getStart().equals(start)
                            && sigmaAggregateEntry.getEnd().equals(end)
            ) {
                return sigmaAggregateEntry;
            }
        }

        SigmaAggregateEntry newValue = new SigmaAggregateEntry(
                start,
                end,
                localValue
                        .f0
                        .getConditionsManager()
                        .getAggregateCondition()
                        .getAggregateValues()
                        .getOperation(),
                0D
        );

        this.localValue.f1.getAggregates().add(newValue);

        return newValue;
    }

    @Override
    public void add(Tuple2<ObjectNode, SigmaRule> value) {

        logger.info("accumulating {} {}", value, localValue);

        try {

            localValue.f0 = value.f1;

            localValue.f1.setSigmaRuleName(value.f1.getTitle());

            SigmaAggregateEntry currentEntry = null;
            for (SigmaAggregateEntry sigmaAggregateEntry : localValue.f1.getAggregates()) {
                if (
                        sigmaAggregateEntry.getStart().equals(value.f0.get("windowStart"))
                                && sigmaAggregateEntry.getEnd().equals(value.f0.get("windowEnd"))
                ) {
                    currentEntry = sigmaAggregateEntry;
                    break;
                }
            }

            if (currentEntry == null) {
                currentEntry = new SigmaAggregateEntry(
                        value.f0.get("windowStart").asLong(),
                        value.f0.get("windowEnd").asLong(),
                        localValue.f0.getConditionsManager().getAggregateCondition().getAggregateValues().getOperation(),
                        0D
                );
                localValue.f1.getAggregates().add(currentEntry);
            }

            switch (value.f1.getConditionsManager().getAggregateCondition().getAggregateValues().getAggregateFunction()) {
                case AggregateParser.COUNT:
                    currentEntry.setOperation("count");
                    currentEntry.setValue(currentEntry.getValue() + 1);
                    break;

                case AggregateParser.SUM:

                    currentEntry.setOperation("sum");
                    currentEntry.setValue(
                            currentEntry.getValue()
                                    + value.f0.get(
                                            value
                                                    .f1
                                                    .getConditionsManager()
                                                    .getAggregateCondition()
                                                    .getAggregateValues()
                                                    .getDistinctValue()
                                    )
                                    .asDouble()
                    );
                    break;

                case AggregateParser.AVG:
                    this.count++;
                    this.sum += value.f0.get(value.f1.getConditionsManager().getAggregateCondition().getAggregateValues().getDistinctValue()).asDouble();
                    currentEntry.setOperation("average");
                    currentEntry.setValue(this.sum / this.count);
                    break;
                case AggregateParser.MIN:
                    currentEntry.setOperation("min");
                    currentEntry.setValue(Math.min(currentEntry.getValue(), value.f0.get(value.f1.getConditionsManager().getAggregateCondition().getAggregateValues().getDistinctValue()).asDouble()));
                    break;
                case AggregateParser.MAX:
                    currentEntry.setOperation("max");
                    currentEntry.setValue(Math.max(currentEntry.getValue(), value.f0.get(value.f1.getConditionsManager().getAggregateCondition().getAggregateValues().getDistinctValue()).asDouble()));
                    break;
                default:
                    throw new UnsupportedOperationException("Sigma Aggregation Operation not supported");
            }
            ;
        } catch (Exception e) {

        }

        logger.info("new local value {}", localValue);
    }

    @Override
    public Tuple2<SigmaRule, SigmaAggregate> getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        localValue = Tuple2.of(null, new SigmaAggregate());
    }

    @Override
    public void merge(Accumulator<Tuple2<ObjectNode, SigmaRule>, Tuple2<SigmaRule, SigmaAggregate>> other) {

        if (!(other instanceof SigmaWindowedStreamAccumulator)) {
            throw new IllegalArgumentException(
                    "The merged accumulator must be SigmaWindowedStreamAccumulator.");
        }

        ((SigmaWindowedStreamAccumulator) other).sum = this.sum + ((SigmaWindowedStreamAccumulator) other).sum;
        ((SigmaWindowedStreamAccumulator) other).count = this.count + ((SigmaWindowedStreamAccumulator) other).count;

        for (SigmaAggregateEntry sigmaAggregateEntry : localValue.f1.getAggregates()) {
            SigmaAggregateEntry otherAggregateEntry = ((SigmaWindowedStreamAccumulator) other).computeIfAbsent(sigmaAggregateEntry.getStart(), sigmaAggregateEntry.getEnd());
            otherAggregateEntry.setOperation(sigmaAggregateEntry.getOperation());
            switch (localValue.f0.getConditionsManager().getAggregateCondition().getAggregateValues().getAggregateFunction()) {
                case AggregateParser.COUNT:
                case AggregateParser.SUM:
                    otherAggregateEntry.setValue(sigmaAggregateEntry.getValue() + otherAggregateEntry.getValue());

                    break;
                case AggregateParser.AVG:
                    int count = ((SigmaWindowedStreamAccumulator) other).count + this.count;
                    double sum = this.sum + ((SigmaWindowedStreamAccumulator) other).sum;
                    Double avg = sum / count;
                    otherAggregateEntry.setValue(avg);
                    break;
                case AggregateParser.MIN:
                    otherAggregateEntry.setValue(Math.min(otherAggregateEntry.getValue(), sigmaAggregateEntry.getValue()));
                    break;
                case AggregateParser.MAX:
                    otherAggregateEntry.setValue(Math.max(otherAggregateEntry.getValue(), sigmaAggregateEntry.getValue()));
                    break;
                default:
                    throw new UnsupportedOperationException("Sigma Aggregation Operation not supported");
            }

        }
    }

    @Override
    public Accumulator<Tuple2<ObjectNode, SigmaRule>, Tuple2<SigmaRule, SigmaAggregate>> clone() {

        SigmaAggregate cloneSigmaAggregate = new SigmaAggregate();
        cloneSigmaAggregate.setSigmaRuleName(this.localValue.f1.getSigmaRuleName());
        for (SigmaAggregateEntry sigmaAggregateEntry : this.localValue.f1.getAggregates()) {
            cloneSigmaAggregate.getAggregates().add(new SigmaAggregateEntry(
                            sigmaAggregateEntry.getStart(),
                            sigmaAggregateEntry.getEnd(),
                            sigmaAggregateEntry.getOperation(),
                            sigmaAggregateEntry.getValue()
                    )
            );

        }
        SigmaWindowedStreamAccumulator clone = new SigmaWindowedStreamAccumulator();

        clone.localValue = Tuple2.of(this.localValue.f0, cloneSigmaAggregate);
        clone.sum = sum;
        clone.count = count;

        return clone;
    }
}
