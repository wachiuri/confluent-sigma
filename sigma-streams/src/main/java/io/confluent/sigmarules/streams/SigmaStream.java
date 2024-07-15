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
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import io.confluent.sigmarules.appState.SigmaAppInstanceStore;
import io.confluent.sigmarules.config.SigmaPropertyEnum;
import io.confluent.sigmarules.exceptions.InvalidSigmaRuleException;
import io.confluent.sigmarules.exceptions.SigmaRuleParserException;
import io.confluent.sigmarules.fieldmapping.FieldMapper;
import io.confluent.sigmarules.models.SigmaRule;
import io.confluent.sigmarules.parsers.SigmaRuleParser;
import io.confluent.sigmarules.rules.SigmaRuleFactoryObserver;
import io.confluent.sigmarules.rules.SigmaRulesFactory;
import io.confluent.sigmarules.utilities.JsonUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

public class SigmaStream extends StreamManager {
    final static Logger logger = LogManager.getLogger(SigmaStream.class);
    final static String instanceId = UUID.randomUUID().toString();
    private final String rulesTopic;

    private KafkaStreams streams;
    StreamExecutionEnvironment env;
    private SigmaRulesFactory ruleFactory;
    private String inputTopic;
    private String outputTopic;
    private Boolean firstMatch;
    private SigmaAppInstanceStore instanceStore;
    private final Configuration jsonPathConf = createJsonPathConfig();
    KStream<String, JsonNode> sigmaStream = null;

    public SigmaStream(Properties properties, SigmaRulesFactory ruleFactory) {
        super(properties);

        this.ruleFactory = ruleFactory;
        //this.instanceStore = new SigmaAppInstanceStore(properties, this);
        this.outputTopic = properties.getProperty(SigmaPropertyEnum.OUTPUT_TOPIC.toString());
        this.inputTopic = properties.getProperty(SigmaPropertyEnum.DATA_TOPIC.toString());
        this.rulesTopic = properties.getProperty(SigmaPropertyEnum.SIGMA_RULES_TOPIC.toString());
        this.firstMatch = Boolean.valueOf(
                properties.getProperty(SigmaPropertyEnum.SIGMA_RULE_FIRST_MATCH.toString()));

        // if the new or updated rule has an aggregate condition, we must either add a new
        // substream (for a new rule) or restart the topology if a rule has been changed
        // substream (for a new rule) or restart the topology if a rule has been changed
        // FF has been entered for dynamic changes to substreams
        ruleFactory.addObserver(new SigmaRuleFactoryObserver() {
            @Override
            public void processRuleUpdate(SigmaRule newRule, SigmaRule oldRule, Boolean newRuleAdded) {
                if (newRule.getConditionsManager().hasAggregateCondition()) {
                    if (newRuleAdded) {
                        logger.info("New aggregate rule: " + newRule.getTitle());
                        streams.close();
                        startStream();
                    } else {
                        // we only need to restart the topology if the window time has changed
                        if (newRule.getDetectionsManager().getWindowTimeMS().equals(
                                oldRule.getDetectionsManager().getWindowTimeMS()) == false) {
                            logger.info(newRule.getTitle() +
                                    " window time has been modified. Restarting topology");
                            streams.close();
                            startStream();
                        }
                    }
                }
            }
        }, false);
    }

    public void startStream() {

        try {
            createTopic(inputTopic);
            createTopic(outputTopic);
            createTopic(rulesTopic);

            //Topology topology = createTopology();

            env = StreamExecutionEnvironment.createLocalEnvironment();
            createFlinkTopology(env);

            //streams = new KafkaStreams(topology, getStreamProperties());

            //instanceStore.register();

            //streams.cleanUp();
            //streams.start();

            env.execute();
        } catch (Exception e) {
            logger.error("Error executing flink stream execution environment", e);
        }

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            //streams.close();
            try {
                env.close();
            } catch (Exception e) {
                logger.error("error shutting down flink stream execution environment", e);
            }
        }));
    }

    public void stopStream() {
        this.streams.close();
        try {
            env.close();
        } catch (Exception e) {
            logger.error("error closing flink stream execution environment", e);
        }
    }

    public void createFlinkTopology(final StreamExecutionEnvironment env) throws IOException {

        KafkaSource<String> dataSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty(SigmaPropertyEnum.BOOTSTRAP_SERVERS.toString()))
                .setTopics(inputTopic)
                .setGroupId(instanceId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<ObjectNode> dataStream = env.fromSource(
                dataSource,
                WatermarkStrategy.noWatermarks(),
                "logs"
        ).map(string -> (ObjectNode) new ObjectMapper().readTree(string));

        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty(SigmaPropertyEnum.BOOTSTRAP_SERVERS.toString()))
                .setTopics(rulesTopic)
                .setGroupId(instanceId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<SigmaRule> ruleStream = env.fromSource(
                        ruleSource,
                        WatermarkStrategy.noWatermarks(),
                        "sigma rules"
                )
                .filter(string -> !string.isBlank())
                .map(fileContent -> {
                    try {
                        SigmaRuleParser sigmaRuleParser = new SigmaRuleParser(
                                new FieldMapper(
                                        "D:\\Paper\\public\\confluent-sigma-1.3.0\\demo\\config\\zeek.yml"
                                )
                        );

                        return sigmaRuleParser.parseRule(fileContent);
                    } catch (InvalidSigmaRuleException | SigmaRuleParserException e) {
                        logger.log(Level.ERROR, "Error parsing rule: " + fileContent, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull);

        // simple rules
        SimpleFlinkTopology simpleFlinkTopology = new SimpleFlinkTopology();
        simpleFlinkTopology.createSimpleFlinkTopology(this, dataStream, ruleStream, outputTopic,
                jsonPathConf, firstMatch);

        // aggregate rules
        AggregateFlinkTopology aggregateFlinkTopology = new AggregateFlinkTopology();
        aggregateFlinkTopology.createAggregateFlinkTopology(this, dataStream, ruleStream, outputTopic,
                jsonPathConf);


    }

    // iterates through each rule and publishes to output topic for
    // each rule that is a match
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        sigmaStream = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), JsonUtils.getJsonSerde()));

        // simple rules
        SimpleTopology simpleTopology = new SimpleTopology();
        simpleTopology.createSimpleTopology(this, sigmaStream, ruleFactory, outputTopic,
                jsonPathConf, firstMatch);

        // aggregate rules
        AggregateTopology aggregateTopology = new AggregateTopology();
        aggregateTopology.createAggregateTopology(this, sigmaStream, ruleFactory, outputTopic,
                jsonPathConf);

        return builder.build();
    }

    public KafkaStreams getStreams() {
        return streams;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public SigmaRulesFactory getRuleFactory() {
        return ruleFactory;
    }

    public static Configuration createJsonPathConfig() {
        return Configuration.builder()
                .mappingProvider(new JacksonMappingProvider()) // Required for JsonNode object
                .jsonProvider(new JacksonJsonProvider()) // Required for JsonNode object
                .options(Option.SUPPRESS_EXCEPTIONS) // Return null when path is not found - https://github.com/json-path/JsonPath#tweaking-configuration
                .build();
    }

}
