/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.confluent.sigmarules.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import io.confluent.sigmarules.flink.accumulator.SigmaWindowedStreamAccumulator;
import io.confluent.sigmarules.models.AggregateValues;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.parsers.AggregateParser;
import io.confluent.sigmarules.rules.SigmaRuleCheck;
import io.confluent.sigmarules.rules.SigmaRulesFactory;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AggregateFlinkTopology extends SigmaBaseTopology {
    final static Logger logger = LogManager.getLogger(AggregateFlinkTopology.class);

    private final SigmaRuleCheck ruleCheck = new SigmaRuleCheck();

    public void createAggregateFlinkTopology(StreamManager streamManager, DataStream<String> sigmaStream,
                                             SigmaRulesFactory ruleFactory, String outputTopic, Configuration jsonPathConf) {

        final Serde<AggregateResults> aggregateSerde = AggregateResults.getJsonSerde();

        for (SigmaRule rule : ruleFactory
                .getSigmaRules()
                .values()
                .stream()
                .filter(
                        sigmaRule -> sigmaRule
                                .getConditionsManager()
                                .hasAggregateCondition()
                ).collect(Collectors.toList())
        ) {

            streamManager.setRecordsProcessed(streamManager.getRecordsProcessed() + 1);

            AggregateValues aggregateValues = rule.getConditionsManager().getAggregateCondition().getAggregateValues();

            sigmaStream
                    .map(sourceData -> (ObjectNode) new ObjectMapper().readTree(sourceData))
                    .filter(data -> ruleCheck.isValid(rule, data, jsonPathConf))
                    .name("filter data that matches the rule " + rule.getTitle())
                    .keyBy(sourceData -> updateKey(rule, sourceData))
                    .process(
                            new KeyedProcessFunction<String, ObjectNode, ObjectNode>() {
                                @Override
                                public void processElement(ObjectNode value, KeyedProcessFunction<String, ObjectNode, ObjectNode>.Context ctx, Collector<ObjectNode> out) throws Exception {
                                    ObjectNode newValue = (ObjectNode) value;
                                    newValue.put("sigmaGroupByKey", ctx.getCurrentKey());
                                    out.collect(newValue);
                                }
                            })
                    .name("put the key (rule-aggregationFunction-distinctValue-operation-operationValue) in the object")
                    .keyBy(node -> node.get("sigmaGroupByKey").asText())
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(rule.getDetectionsManager().getWindowTimeMS())))
                    .process(new ProcessWindowFunction<ObjectNode, ObjectNode, String, TimeWindow>() {
                        @Override
                        public void process(String key, ProcessWindowFunction<ObjectNode, ObjectNode, String, TimeWindow>.Context context, Iterable<ObjectNode> elements, Collector<ObjectNode> out) throws Exception {
                            elements.forEach(objectNode -> {

                                objectNode.put("sigmaTimeFrame", context.window().getStart() + "-" + context.window().getEnd());
                                out.collect(objectNode);
                            });
                        }
                    })
                    .name("put the time frame in the object")
                    .keyBy(node -> node.get("sigmaGroupByKey").asText() + "-" + node.get("sigmaTimeFrame").asText())
                    .window(TumblingEventTimeWindows.of(Duration.ofMillis(rule.getDetectionsManager().getWindowTimeMS())))
                    .aggregate(getAggregateFunction(aggregateValues))
                    .name("aggregate")
                    .map(aggregate -> filterMatchingAggregations(rule, aggregate))
                    .name("filter matching aggregations")
                    .filter(aggregate -> !aggregate.isEmpty())
                    .name("filter empty aggregations")
                    .map(aggregate -> new ObjectMapper().writeValueAsString(aggregate))
                    .name("convert results to json")
                    .sinkTo(
                            KafkaSink.<String>builder()
                                    .setBootstrapServers(streamManager.getStreamProperties().getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
                                    .setRecordSerializer(
                                            KafkaRecordSerializationSchema.builder()
                                                    .setTopic(outputTopic)
                                                    .setValueSerializationSchema(new SimpleStringSchema())
                                                    .build()
                                    )
                                    .build()
                    )
                    .name("send results to kafka output topic")
            ;
        }
    }

    private AggregateFunction<ObjectNode, ? extends Accumulator<JsonNode, HashMap<String, HashMap<String, Double>>>, HashMap<String, HashMap<String, Double>>> getAggregateFunction(AggregateValues aggregateValues) {

        return new AggregateFunction<ObjectNode, SigmaWindowedStreamAccumulator, HashMap<String, HashMap<String, Double>>>() {

            @Override
            public SigmaWindowedStreamAccumulator createAccumulator() {
                return new SigmaWindowedStreamAccumulator(aggregateValues);
            }

            @Override
            public SigmaWindowedStreamAccumulator add(ObjectNode value, SigmaWindowedStreamAccumulator accumulator) {
                String distinctValue = aggregateValues.getDistinctValue();

                if (!value.has(distinctValue)) {
                    return accumulator;
                }

                accumulator.add(value);

                return accumulator;
            }

            @Override
            public HashMap<String, HashMap<String, Double>> getResult(SigmaWindowedStreamAccumulator accumulator) {
                return accumulator.getLocalValue();
            }

            @Override
            public SigmaWindowedStreamAccumulator merge(SigmaWindowedStreamAccumulator a, SigmaWindowedStreamAccumulator b) {
                a.merge(b);
                return a;
            }
        };
    }

    private String updateKey(SigmaRule rule, JsonNode source) {
        // make the key the title + groupBy + distinctValue, so we have 1 unique stream
        //count(username) by sourceIp>2
        AggregateValues aggregateValues = rule.getConditionsManager().getAggregateCondition()
                .getAggregateValues();

        String newKey = rule.getTitle();
        String groupBy = aggregateValues.getGroupBy();
        if (groupBy != null && !groupBy.isEmpty() && source.get(groupBy) != null) {
            newKey = newKey + "-" + source.get(aggregateValues.getGroupBy()).asText();
        }

        String distinctValue = aggregateValues.getDistinctValue();
        if (distinctValue != null && !distinctValue.isEmpty() &&
                source.get(distinctValue) != null) {
            newKey = newKey + "-" + source.get(distinctValue).asText();
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

    private HashMap<String, HashMap<String, Double>> filterMatchingAggregations(SigmaRule rule, HashMap<String, HashMap<String, Double>> aggregations) {
        AggregateValues aggregateValues = rule.getConditionsManager().getAggregateCondition()
                .getAggregateValues();
        long operationValue = Long.parseLong(aggregateValues.getOperationValue());

        HashMap<String, HashMap<String, Double>> matchedGroupByData = new HashMap<>();

        for (Map.Entry<String, HashMap<String, Double>> groupBys : aggregations.entrySet()) {

            HashMap<String, Double> matchedWindowData = new HashMap<>();

            for (Map.Entry<String, Double> sigmaTimeFrameMap : groupBys.getValue().entrySet()) {

                switch (aggregateValues.getOperation()) {
                    case AggregateParser.EQUALS:
                        if (sigmaTimeFrameMap.getValue() == operationValue) {
                            matchedWindowData.put(sigmaTimeFrameMap.getKey(), sigmaTimeFrameMap.getValue());
                        }
                        break;
                    case AggregateParser.GREATER_THAN:
                        if (sigmaTimeFrameMap.getValue() > operationValue) {
                            matchedWindowData.put(sigmaTimeFrameMap.getKey(), sigmaTimeFrameMap.getValue());
                        }
                        break;
                    case AggregateParser.GREATER_THAN_EQUAL:
                        if (sigmaTimeFrameMap.getValue() >= operationValue) {
                            matchedWindowData.put(sigmaTimeFrameMap.getKey(), sigmaTimeFrameMap.getValue());
                        }
                        break;
                    case AggregateParser.LESS_THAN:
                        if (sigmaTimeFrameMap.getValue() < operationValue) {
                            matchedWindowData.put(sigmaTimeFrameMap.getKey(), sigmaTimeFrameMap.getValue());
                        }
                        break;
                    case AggregateParser.LESS_THAN_EQUAL:
                        if (sigmaTimeFrameMap.getValue() <= operationValue) {
                            matchedWindowData.put(sigmaTimeFrameMap.getKey(), sigmaTimeFrameMap.getValue());
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unhandled operation: " + aggregateValues.getOperation() +
                                ". For rule " + rule.getTitle());
                }
            }

            if (!matchedWindowData.isEmpty()) {
                matchedGroupByData.put(groupBys.getKey(), matchedWindowData);
            }
        }

        return matchedGroupByData;
    }
}
