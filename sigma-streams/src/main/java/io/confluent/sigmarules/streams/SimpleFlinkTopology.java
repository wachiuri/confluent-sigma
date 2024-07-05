/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.confluent.sigmarules.streams;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.rules.SigmaRuleCheck;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimpleFlinkTopology extends SigmaBaseTopology {
    private static final long MINUTE_IN_MILLIS = 60 * 1000;
    final static Logger logger = LogManager.getLogger(SimpleFlinkTopology.class);

    private long matches = 0;
    private long lastCallTime = 0;

    public void createSimpleFlinkTopology(StreamManager streamManager, DataStream<ObjectNode> sigmaStream,
                                          DataStream<SigmaRule> rulesStream, String outputTopic, Configuration jsonPathConf,
                                          Boolean firstMatch) {

        setDefaultOutputTopic(outputTopic);

        rulesStream
                .join(sigmaStream)
                .where(rule -> "same")
                .equalTo(logItem -> "same")
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<SigmaRule, ObjectNode, Tuple2<SigmaRule, ObjectNode>>() {
                    @Override
                    public Tuple2<SigmaRule, ObjectNode> join(SigmaRule rule, ObjectNode log) throws Exception {
                        return Tuple2.of(rule, log);
                    }
                })
                .filter(new FilterFunction<Tuple2<SigmaRule, ObjectNode>>() {
                    @Override
                    public boolean filter(Tuple2<SigmaRule, ObjectNode> tuple2) throws Exception {
                        return new SigmaRuleCheck().isValid(tuple2.f0, tuple2.f1);
                    }
                })
                .map(tuple2 -> tuple2.f1.toString())
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
                .name("send results to kafka output topic: " + outputTopic)
        ;

        /*Set<SigmaRule> sigmaRules = new HashSet<>(ruleFactory.getSigmaRules().values())
                .stream().filter(rule -> rule.getConditionsManager().hasAggregateCondition())
                .collect(Collectors.toSet());
        ;


        sigmaStream
                .flatMap(new FlatMapFunction<ObjectNode, DetectionResults>() {
                    @Override
                    public void flatMap(ObjectNode sourceData, Collector<DetectionResults> collector) throws Exception {

                        for (SigmaRule rule : sigmaRules) {

                            logger.debug("check rule " + rule.getTitle());
                           // streamManager.setRecordsProcessed(streamManager.getRecordsProcessed() + 1);

                            if (ruleCheck.isValid(rule, sourceData, jsonPathConf)) {
                                collector.collect(buildResults(rule, sourceData));

                                if (logger.getLevel().isLessSpecificThan(Level.WARN)) {
                                    matches++;
                                    long currentTime = System.currentTimeMillis();
                                    if (currentTime - lastCallTime > MINUTE_IN_MILLIS) {
                                        lastCallTime = currentTime;
                                        logger.log(Level.INFO, "Number of matches " + matches);
                                    }
                                }

                                streamManager.setNumMatches(streamManager.getNumMatches() + 1);

                                if (firstMatch)
                                    break;
                            }
                        }
                    }
                })
                .name("matching data against sigma rules")
                .map(detectionResult -> new ObjectMapper().writeValueAsString(detectionResult))
                .name("convert results to json")
                .sinkTo(
                        KafkaSink.<String>builder()
                                .setBootstrapServers(streamManager.getStreamProperties().getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG))
                                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                        .setTopic(outputTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build()
                                )
                                .build()
                )
                .name("send results to kafka output topic")
        ;
        */
    }

}
