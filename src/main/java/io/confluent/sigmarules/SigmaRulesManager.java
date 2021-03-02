package io.confluent.sigmarules;

import java.util.Properties;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.kafka.common.serialization.Serdes;

import io.confluent.sigmarules.models.SigmaFSConnector;
import io.confluent.sigmarules.models.SigmaRule;
import io.kcache.Cache;
import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;

public class SigmaRulesManager {
    private Cache<String, String> sigmaRulesCache;

    public SigmaRulesManager(String bootStrapServers) {
        Properties props = new Properties();
        props.setProperty("kafkacache.bootstrap.servers", bootStrapServers);
        props.setProperty("kafkacache.topic", "sigma_rules_cache");
        sigmaRulesCache = new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), Serdes.String());
        sigmaRulesCache.init();
    }

    public void createSigmaRuleStream() {
        SigmaRawStream sigmaStream = new SigmaRawStream(this);
        sigmaStream.startStream();
    }

    public void addRule(String ruleName, String rule) {
        sigmaRulesCache.put(ruleName, rule);
    }

    public void removeRule(String ruleName) {
        sigmaRulesCache.remove(ruleName);
    }

    public Set<String> getRuleNames() {
        return sigmaRulesCache.keySet();
    }

    public String getRuleAsYaml(String ruleName) {
        return sigmaRulesCache.get(ruleName);
    }

    public SigmaRule getRule(String ruleName) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        SigmaRule sigmaRule = null;

        try {
            String rule = getRuleAsYaml(ruleName);
            if (rule != null) {
                SigmaFSConnector sigmaFSConnector = mapper.readValue(rule, SigmaFSConnector.class);
                sigmaRule = sigmaFSConnector.getPayload();
            }
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        };

        return sigmaRule;
    }

    public static void main(String[] args) {
        // TODO: add config as arguments
        SigmaRulesManager rulesManager = new SigmaRulesManager("127.0.0.1:9092");
        rulesManager.createSigmaRuleStream();

        while (true) {
            try {
                System.out.println("Number of rules: " + rulesManager.getRuleNames().size());

                String ruleName = "Executable from Webdav";
                if (rulesManager.getRule("Executable from Webdav") != null) {
                    System.out.println("Rule as YAML: " + rulesManager.getRuleAsYaml(ruleName));
                    System.out.println("Rule as POJO: title: " + rulesManager.getRule(ruleName).getTitle());
                }

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }

    }

}
