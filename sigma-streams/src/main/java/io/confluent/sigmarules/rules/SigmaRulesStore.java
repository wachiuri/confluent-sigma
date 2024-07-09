package io.confluent.sigmarules.rules;

import io.confluent.sigmarules.parsers.ParsedSigmaRule;
import io.kcache.CacheUpdateHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Stream;

public interface SigmaRulesStore extends CacheUpdateHandler<String, ParsedSigmaRule> {

    public void addObserver (SigmaRuleObserver observer);

    public Set<String> getRuleNames();

    public String getRuleAsYaml(String ruleName);

    public SortedMap<String, ParsedSigmaRule> getRules();

    public Stream<Map.Entry<String, ParsedSigmaRule>> getRulesStream();

    public List<ParsedSigmaRule> getRulesList();

    public void addRule(String rule);


}
