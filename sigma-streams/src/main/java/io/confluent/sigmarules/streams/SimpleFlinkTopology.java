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

import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import io.confluent.sigmarules.flink.sink.ElasticSearch5Sink;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.rules.SigmaRuleCheck;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class SimpleFlinkTopology extends SigmaBaseTopology {
    private static final long MINUTE_IN_MILLIS = 60 * 1000;
    final static Logger logger = LogManager.getLogger(SimpleFlinkTopology.class);

    private long matches = 0;
    private long lastCallTime = 0;

    public void createSimpleFlinkTopology(StreamManager streamManager, DataStream<ObjectNode> dataStream,
                                          DataStream<SigmaRule> ruleStream, String outputTopic, Configuration jsonPathConf,
                                          Boolean firstMatch) throws IOException {

        setDefaultOutputTopic(outputTopic);

        SigmaRuleCheck sigmaRuleCheck = new SigmaRuleCheck();

        String elasticSearchHost = streamManager.getStreamProperties().getProperty("elasticsearch.host");
        int elasticSearchPort = Integer.parseInt(streamManager.getStreamProperties().getProperty("elasticsearch.port"));
        String elasticSearchIndex = streamManager.getStreamProperties().getProperty("elasticsearch.outputindex");
        String elasticSearchDocType = streamManager.getStreamProperties().getProperty("elasticsearch.outputdoctype");
        String elasticSearchScheme = streamManager.getStreamProperties().getProperty("elasticsearch.scheme");
        String clusterName = streamManager.getStreamProperties().getProperty("elasticsearch.cluster.name");
        int elasticSearchTimeout = Integer.parseInt(streamManager.getStreamProperties().getProperty("elasticsearch.timeout"));
        int adminPort = Integer.parseInt(streamManager.getStreamProperties().getProperty("elasticsearch.admin.port"));

        dataStream
                .join(ruleStream.filter(rule -> !rule.getConditionsManager().hasAggregateCondition()))
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
                .filter(tuple2 -> sigmaRuleCheck.isValid(tuple2.f1, tuple2.f0))
                .name("matching data against sigma rules")
                .map(tuple2 -> tuple2.f0)
                .name("extract data")
                .map(BaseJsonNode::toString)
                .name("convert results to json string")
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
                .name("write to file")
        ;
    }
}
