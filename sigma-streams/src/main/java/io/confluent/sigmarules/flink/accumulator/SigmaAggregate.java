package io.confluent.sigmarules.flink.accumulator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SigmaAggregate {

    private String sigmaRuleName;
    private List<SigmaAggregateEntry> aggregates=new ArrayList<>();

    public String getSigmaRuleName() {
        return sigmaRuleName;
    }

    public void setSigmaRuleName(String sigmaRuleName) {
        this.sigmaRuleName = sigmaRuleName;
    }

    public List<SigmaAggregateEntry> getAggregates() {
        return aggregates;
    }

    public void setAggregates(List<SigmaAggregateEntry> aggregates) {
        this.aggregates = aggregates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SigmaAggregate)) return false;
        SigmaAggregate that = (SigmaAggregate) o;
        return Objects.equals(sigmaRuleName, that.sigmaRuleName) && Objects.equals(aggregates, that.aggregates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sigmaRuleName, aggregates);
    }

    @Override
    public String toString() {
        return "SigmaAggregate{" +
                "sigmaRuleName='" + sigmaRuleName + '\'' +
                ", aggregates=" + aggregates +
                '}';
    }
}
