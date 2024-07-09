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

package io.confluent.sigmarules.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.sigmarules.config.SigmaOptions;
import io.confluent.sigmarules.rules.SigmaFileRulesStore;
import io.confluent.sigmarules.rules.SigmaRulesStore;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Application to load sigma rules from a filesystem and put it into the sigma rules topic.
 */
public class SigmaRuleLoader {
    final static Logger logger = LogManager.getLogger(SigmaRuleLoader.class);

    private SigmaRulesStore sigmaRulesStore;
    private ObjectMapper mapper;

    public SigmaRuleLoader(SigmaOptions options) {
        sigmaRulesStore = new SigmaFileRulesStore();
        mapper = new ObjectMapper(new YAMLFactory());
    }

    public void loadSigmaFile(String filename) {
        try {
            String rule = Files.readString(Path.of(filename));
            sigmaRulesStore.addRule(rule);
        } catch (IOException e) {
            logger.error("Failed to load: " + filename, e);
        }
    }

    public void loadSigmaDirectory(String dirName) {
        try (Stream<Path> walk = Files.walk(Paths.get(dirName))) {

            Map<Boolean, List<Path>> isRegularFile = walk.collect(Collectors.partitioningBy(path -> new File(String.valueOf(path)).isFile()));

            for (Path file : isRegularFile.get(true)) {
                logger.log(Level.INFO, "loading file " + file);
                loadSigmaFile(file.toString());
            }

            for (Path directory : isRegularFile.get(false)) {
                logger.log(Level.INFO,"loading directory " + directory);
                loadSigmaDirectory(directory.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void setOptions(Options options) {
        options.addOption("f", "file", true, "Path to sigma rule file.");
        options.addOption("d", "dir", true, "Path to directory contain sigma rules.");
        options.addOption("c", "config", true, "Path to properties file");
    }

    public static void main(String[] args) {
        Options options = new Options();
        setOptions(options);

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args, false);

            if ((cmd.hasOption("c")) && (cmd.hasOption("f") || cmd.hasOption("d"))) {
                InputStream input = new FileInputStream(cmd.getOptionValue("c"));
                Properties properties = new Properties();
                properties.load(input);

                SigmaOptions sigmaOptions = new SigmaOptions();
                sigmaOptions.setProperties(properties);

                SigmaRuleLoader sigma = new SigmaRuleLoader(sigmaOptions);
                if (cmd.hasOption("f")) {
                    sigma.loadSigmaFile(cmd.getOptionValue("f"));
                }

                if (cmd.hasOption("d")) {
                    sigma.loadSigmaDirectory(cmd.getOptionValue("d"));
                }
            } else {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("sigmal_rule_loader", options, true);

                System.exit(0);
            }
        } catch (ParseException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.exit(0);
    }
}
