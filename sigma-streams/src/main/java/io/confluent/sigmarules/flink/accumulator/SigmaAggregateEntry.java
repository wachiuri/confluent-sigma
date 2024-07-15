package io.confluent.sigmarules.flink.accumulator;

import java.util.Objects;

public class SigmaAggregateEntry {

    private Long start;
    private Long end;
    private String operation;
    private Double value;

    public SigmaAggregateEntry(Long start, Long end, String operation, Double value) {
        this.start = start;
        this.end = end;
        this.operation = operation;
        this.value = value;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SigmaAggregateEntry)) return false;
        SigmaAggregateEntry that = (SigmaAggregateEntry) o;
        return Objects.equals(start, that.start) && Objects.equals(end, that.end) && Objects.equals(operation, that.operation) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, operation, value);
    }

    @Override
    public String toString() {
        return "SigmaAggregateEntry{" +
                "start=" + start +
                ", end=" + end +
                ", operation='" + operation + '\'' +
                ", value=" + value +
                '}';
    }
}
