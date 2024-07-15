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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import io.confluent.sigmarules.flink.accumulator.SigmaAggregate;
import io.confluent.sigmarules.flink.accumulator.SigmaAggregateEntry;
import io.confluent.sigmarules.flink.accumulator.SigmaWindowedStreamAccumulator;
import io.confluent.sigmarules.flink.keyselector.SigmaKeySelector;
import io.confluent.sigmarules.flink.sink.ElasticSearch5Sink;
import io.confluent.sigmarules.flink.windowassigner.SigmaAggregateWindowAssigner;
import io.confluent.sigmarules.models.AggregateValues;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.parsers.AggregateParser;
import io.confluent.sigmarules.rules.SigmaRuleCheck;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class AggregateFlinkTopology extends SigmaBaseTopology {

    final static Logger logger = LoggerFactory.getLogger(AggregateFlinkTopology.class);

    public void createAggregateFlinkTopology(StreamManager streamManager, DataStream<ObjectNode> dataStream,
                                             DataStream<SigmaRule> ruleStream, String outputTopic, Configuration jsonPathConf) throws IOException {

        SigmaRuleCheck ruleCheck = new SigmaRuleCheck();

        String elasticSearchHost = streamManager.getStreamProperties().getProperty("elasticsearch.host");
        int elasticSearchPort = Integer.parseInt(streamManager.getStreamProperties().getProperty("elasticsearch.port"));
        String elasticSearchIndex = streamManager.getStreamProperties().getProperty("elasticsearch.aggregate.outputindex");
        String elasticSearchDocType = streamManager.getStreamProperties().getProperty("elasticsearch.aggregate.outputdoctype");
        String elasticSearchScheme = streamManager.getStreamProperties().getProperty("elasticsearch.scheme");
        String clusterName = streamManager.getStreamProperties().getProperty("elasticsearch.cluster.name");
        int elasticSearchTimeout = Integer.parseInt(streamManager.getStreamProperties().getProperty("elasticsearch.timeout"));
        int adminPort = Integer.parseInt(streamManager.getStreamProperties().getProperty("elasticsearch.admin.port"));

        dataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ObjectNode>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner(
                                        (element, timestamp) -> element.has("timestamp") ?
                                                element.get("timestamp").asLong() :
                                                element.has("ts") ?
                                                        element.get("ts").asLong() :
                                                        timestamp
                                )
                                .withIdleness(Duration.ofMinutes(1))
                )
                .join(ruleStream.filter(rule -> rule.getConditionsManager().hasAggregateCondition()))
                .where(data -> "same")
                .equalTo(rule -> "same")
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<ObjectNode, SigmaRule, Tuple2<ObjectNode, SigmaRule>>() {
                    @Override
                    public Tuple2<ObjectNode, SigmaRule> join(ObjectNode first, SigmaRule second) throws Exception {
                        return Tuple2.of(first, second);
                    }
                })
                .filter(tuple -> {
                    logger.info("comparing rule against data {} {}", tuple.f1, tuple.f0);
                    return ruleCheck.isValid(tuple.f1, tuple.f0);
                })
                .name("filter data that matches the rule ")
                .keyBy(new SigmaKeySelector())
                .window(new SigmaAggregateWindowAssigner())
                .trigger(ProcessingTimeTrigger.create())
                .aggregate(getAggregateFunction())
                .name("aggregate")
                .map(aggregate -> {
                    logger.info("filtering aggregate {}", aggregate);
                    AggregateValues aggregateValues = aggregate.f0.getConditionsManager().getAggregateCondition()
                            .getAggregateValues();
                    long operationValue = Long.parseLong(aggregateValues.getOperationValue());

                    List<SigmaAggregateEntry> matched = new ArrayList<>();

                    for (SigmaAggregateEntry sigmaAggregateEntry : aggregate.f1.getAggregates()) {

                        switch (aggregateValues.getOperation()) {
                            case AggregateParser.EQUALS:
                                if (sigmaAggregateEntry.getValue() == operationValue) {
                                    matched.add(sigmaAggregateEntry);
                                }
                                break;
                            case AggregateParser.GREATER_THAN:
                                if (sigmaAggregateEntry.getValue() > operationValue) {
                                    matched.add(sigmaAggregateEntry);
                                }
                                break;
                            case AggregateParser.GREATER_THAN_EQUAL:
                                if (sigmaAggregateEntry.getValue() >= operationValue) {
                                    matched.add(sigmaAggregateEntry);
                                }
                                break;
                            case AggregateParser.LESS_THAN:
                                if (sigmaAggregateEntry.getValue() < operationValue) {
                                    matched.add(sigmaAggregateEntry);
                                }
                                break;
                            case AggregateParser.LESS_THAN_EQUAL:
                                if (sigmaAggregateEntry.getValue() <= operationValue) {
                                    matched.add(sigmaAggregateEntry);
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException("Unhandled operation: " + aggregateValues.getOperation() +
                                        ". For rule " + aggregate.f0.getTitle());
                        }
                    }

                    aggregate.f1.setAggregates(matched);

                    return aggregate.f1;
                })
                .returns(new TypeHint<SigmaAggregate>() {
                })
                .name("filter matching aggregations")
                .filter(aggregate -> {
                    logger.info("filtering empty aggregates {}", aggregate);
                    return !aggregate.getAggregates().isEmpty();
                })
                .name("filter empty aggregations")
                .map(aggregate->new ObjectMapper().writeValueAsString(aggregate))
                .name("map to json")
                .sinkTo(
                        new ElasticSearch5Sink<>(
                                elasticSearchHost,
                                elasticSearchPort,
                                elasticSearchIndex,
                                elasticSearchDocType,
                                elasticSearchScheme,
                                elasticSearchTimeout,
                                clusterName,
                                adminPort
                        )
                )
                .name("write results to elasticsearch")
        ;

    }

    private AggregateFunction<Tuple2<ObjectNode, SigmaRule>, ? extends Accumulator<Tuple2<ObjectNode, SigmaRule>, Tuple2<SigmaRule, SigmaAggregate>>, Tuple2<SigmaRule, SigmaAggregate>> getAggregateFunction() {

        return new AggregateFunction<Tuple2<ObjectNode, SigmaRule>, SigmaWindowedStreamAccumulator, Tuple2<SigmaRule, SigmaAggregate>>() {

            @Override
            public SigmaWindowedStreamAccumulator createAccumulator() {
                return new SigmaWindowedStreamAccumulator();
            }

            @Override
            public SigmaWindowedStreamAccumulator add(Tuple2<ObjectNode, SigmaRule> value, SigmaWindowedStreamAccumulator accumulator) {

                logger.info("adding to aggregate {}", value);

                accumulator.add(value);

                return accumulator;
            }

            @Override
            public Tuple2<SigmaRule, SigmaAggregate> getResult(SigmaWindowedStreamAccumulator accumulator) {
                logger.info("getResult {} {}", accumulator.getLocalValue(), accumulator);
                return accumulator.getLocalValue();
            }

            @Override
            public SigmaWindowedStreamAccumulator merge(SigmaWindowedStreamAccumulator a, SigmaWindowedStreamAccumulator b) {
                a.merge(b);
                return a;
            }
        };
    }
}
