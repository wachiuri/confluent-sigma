package io.confluent.sigmarules.flink.windowassigner;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.sigmarules.models.SigmaRule;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

public class SigmaAggregateWindowAssigner extends WindowAssigner<Tuple2<ObjectNode, SigmaRule>, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(SigmaAggregateWindowAssigner.class);

    @Override
    public Collection<TimeWindow> assignWindows(Tuple2<ObjectNode, SigmaRule> element, long timestamp, WindowAssignerContext context) {

        long size = element.f1.getDetectionsManager().getWindowTimeMS();
        long globalOffset = 0;
        WindowStagger windowStagger = WindowStagger.ALIGNED;
        Long staggerOffset = null;

        timestamp = element.f0.has("timestamp") ?
                element.f0.get("timestamp").asLong() :
                element.f0.has("ts") ?
                        element.f0.get("ts").asLong() :
                        timestamp
        ;

        if (timestamp > Long.MIN_VALUE) {
            if (staggerOffset == null) {
                staggerOffset =
                        windowStagger.getStaggerOffset(context.getCurrentProcessingTime(), size);
            }
            // Long.MIN_VALUE is currently assigned when no timestamp is present
            long start =
                    TimeWindow.getWindowStartWithOffset(
                            timestamp, (globalOffset + staggerOffset) % size, size);

            element.f0.put("window", start + "-" + (start + size));
            element.f0.put("windowStart", start);
            element.f0.put("windowEnd", start + size);
            return Collections.singletonList(new TimeWindow(start, start + size));
        } else {
            throw new RuntimeException(
                    "Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
                            + "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
                            + "'DataStream.assignTimestampsAndWatermarks(...)'?");
        }
    }

    @Override
    @Deprecated
    public Trigger getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}
