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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.rules.SigmaRuleCheck;
import io.confluent.sigmarules.rules.SigmaRulesFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class SimpleFlinkTopology extends SigmaBaseTopology {
    private static final long MINUTE_IN_MILLIS = 60 * 1000;
    final static Logger logger = LogManager.getLogger(SimpleFlinkTopology.class);

    private long matches = 0;
    private long lastCallTime = 0;

    public void createSimpleFlinkTopology(StreamManager streamManager, DataStream<ObjectNode> sigmaStream,
                                          SigmaRulesFactory ruleFactory, String outputTopic, Configuration jsonPathConf,
                                          Boolean firstMatch) {

        setDefaultOutputTopic(outputTopic);

        Set<SigmaRule> sigmaRules = new HashSet<>(ruleFactory.getSigmaRules().values())
                .stream().filter(rule -> !rule.getConditionsManager().hasAggregateCondition())
                .collect(Collectors.toSet());

        logger.log(Level.INFO, "sigma rules: " + sigmaRules);

        sigmaStream
                .flatMap(new RuleMatch(sigmaRules.toArray(new SigmaRule[0]), firstMatch))
                .name("matching data against sigma rules")
                .map(detectionResult -> new ObjectMapper().writeValueAsString(detectionResult))
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
                .name("send results to kafka output topic " + outputTopic)
        ;
    }

    private class RuleMatch implements FlatMapFunction<ObjectNode, ObjectNode> {

        private final SigmaRule[] sigmaRules;
        private final boolean firstMatch;

        public RuleMatch(SigmaRule[] sigmaRules, boolean firstMatch) {
            this.sigmaRules = sigmaRules;
            this.firstMatch = firstMatch;
        }

        @Override
        public void flatMap(ObjectNode sourceData, Collector<ObjectNode> collector) throws Exception {
            SigmaRuleCheck ruleCheck = new SigmaRuleCheck();
            LogManager.getLogger(SimpleFlinkTopology.class).log(Level.INFO, "source data: " + sourceData.toString());
            for (SigmaRule rule : sigmaRules) {
                // streamManager.setRecordsProcessed(streamManager.getRecordsProcessed() + 1);

                if (ruleCheck.isValid(rule, sourceData)) {
                    LogManager.getLogger(SimpleFlinkTopology.class).log(Level.INFO, "successfully matched data: " + sourceData);
                    LogManager.getLogger(SimpleFlinkTopology.class).log(Level.INFO, "successfully matched rule: " + rule.toString());
                    collector.collect(sourceData);
                    //streamManager.setNumMatches(streamManager.getNumMatches() + 1);

                    if (firstMatch)
                        break;
                }
            }
        }
    }

}
