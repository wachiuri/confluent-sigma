package io.confluent.sigmarules.rules;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.confluent.sigmarules.parsers.ParsedSigmaRule;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SigmaFileRulesStore implements SigmaRulesStore {

    private static final Logger logger = LoggerFactory.getLogger(SigmaFileRulesStore.class);

    private SigmaRuleObserver observer = null;
    private Path path = Path.of("/var/cache/sigmarules/");

    public SigmaFileRulesStore() {
        if (!this.path.toFile().exists()) {
            this.path.toFile().mkdirs();
        }
    }


    @Override
    public void addObserver(SigmaRuleObserver observer) {
        this.observer = observer;
    }

    @Override
    public Set<String> getRuleNames() {
        return getRulesStream().map(parsedSigmaRuleEntry -> parsedSigmaRuleEntry.getKey()).collect(Collectors.toSet());
    }

    @Override
    public String getRuleAsYaml(String ruleName) {
        return getRulesStream()
                .filter(parsedSigmaRuleEntry -> parsedSigmaRuleEntry.getKey().equals(ruleName))
                .findFirst()
                .map(Map.Entry::getValue)
                .map(ParsedSigmaRule::toString)
                .orElse(null);
    }

    @Override
    public SortedMap<String, ParsedSigmaRule> getRules() {
        return getRulesStream()
                .collect(
                        Collectors
                                .toMap(
                                        Map.Entry::getKey,
                                        Map.Entry::getValue,
                                        (existing, replacement) -> existing,  // Merge function to handle duplicates, if any
                                        TreeMap::new
                                )
                );
    }

    private Stream<Map.Entry<String, ParsedSigmaRule>> getRulesStream(Path directoryPath) {
        try {

            try (Stream<Map.Entry<String, ParsedSigmaRule>> rules = Files.walk(directoryPath)
                    .filter(Files::isRegularFile)
                    .filter(x -> x.toString().endsWith(".yaml") || x.toString().endsWith(".yml"))
                    .map(filePath -> {
                        try {
                            return Files.readString(filePath);
                        } catch (IOException e) {
                            logger.error("Error reading file " + filePath, e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .map(content -> {
                        ObjectMapper mapper = new ObjectMapper((JsonFactory) new YAMLFactory());
                        try {
                            return mapper.readValue(content, ParsedSigmaRule.class);
                        } catch (JsonProcessingException e) {
                            logger.error("Error parsing file " + content, e);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .map(parsedSigmaRule -> Map.entry(parsedSigmaRule.getTitle(), parsedSigmaRule))) {

                try (Stream<Map.Entry<String, ParsedSigmaRule>> rulesInSubDirectories = Files.walk(directoryPath)
                        .filter(Files::isDirectory)
                        .flatMap(dirPath -> this.getRulesStream(directoryPath.resolve(dirPath)))) {

                    return Stream.concat(rules, rulesInSubDirectories);
                }

            }

        } catch (IOException e) {
            logger.error("Error reading directory " + path, e);
            return Stream.empty();
        }
    }

    @Override
    public Stream<Map.Entry<String, ParsedSigmaRule>> getRulesStream() {
        return getRulesStream(path);
    }

    @Override
    public List<ParsedSigmaRule> getRulesList() {
        return getRulesStream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList())
                ;
    }

    @Override
    public void addRule(String rule) {
        ObjectMapper mapper = new ObjectMapper((JsonFactory) new YAMLFactory());
        ParsedSigmaRule sigmaRule = null;
        try {
            if (rule != null) {
                sigmaRule = mapper.readValue(rule, ParsedSigmaRule.class);
                File file = new File(path.toFile(), sigmaRule.getTitle() + ".yaml");

                if (!file.exists()) {
                    file.createNewFile();
                }

                FileOutputStream fileOutputStream = new FileOutputStream(file);
                fileOutputStream.write(rule.getBytes());
                fileOutputStream.close();

            }
        } catch (IOException e) {
            logger.error("Error adding rule", e);
        }
    }

    @Override
    public void handleUpdate(String key, ParsedSigmaRule value, ParsedSigmaRule oldValue,
                             TopicPartition tp, long offset, long timestamp) {
        if (!value.equals(oldValue)) {
            if (observer != null) {
                observer.handleRuleUpdate(key, value, oldValue);
            }
        }
    }
}
