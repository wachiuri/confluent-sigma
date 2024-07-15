package io.confluent.sigmarules.flink.sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import java.util.UUID;

import static java.util.Map.entry;

public class ElasticSearch5Sink<T> implements Serializable, Sink<T> {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearch5Sink.class);

    private final String host;
    private final int port;
    private final String index;
    private final String docType;
    private final String scheme;
    private final int timeout;
    private final String clusterName;
    private final int adminPort;

    public ElasticSearch5Sink(
            String host,
            int port,
            String index,
            String docType,
            String scheme,
            int timeout,
            String clusterName,
            int adminPort
    ) throws IOException {

        this.host = host;
        this.port = port;
        this.index = index;
        this.docType = docType;
        this.scheme = scheme;
        this.timeout = timeout;
        this.clusterName = clusterName;
        this.adminPort = adminPort;

        manageIndices();
    }

    private void manageIndices() throws IOException {

        Settings settings = Settings.builder()
                .put("cluster.name", clusterName).build();
        TransportClient transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), adminPort));

        manageIndex("aggregate", "aggregate",
                Map.of(
                        "properties", Map.of(
                                "sigmaRuleName", Map.of("type", "text"),
                                "aggregates", Map.of(
                                        "type", "nested",
                                        "properties", Map.of(
                                                "startTimestamp", Map.of("type", "long"),
                                                "endTimestamp", Map.of("type", "long"),
                                                "operation", Map.of("type", "keyword"),
                                                "value", Map.of("type", "integer")
                                        )
                                )
                        )
                ),
                transportClient
        );

        manageIndex("sigmadetection", "sigmadetection", Map.of(
                        "properties", Map.ofEntries(
                                entry("ts", Map.of("type", "double")),
                                entry("uid", Map.of("type", "keyword")),
                                entry("id.orig_h", Map.of("type", "ip")),
                                entry("id.orig_p", Map.of("type", "integer")),
                                entry("id.resp_h", Map.of("type", "ip")),
                                entry("id.resp_p", Map.of("type", "integer")),
                                entry("proto", Map.of("type", "keyword")),
                                entry("trans_id", Map.of("type", "integer")),
                                entry("query", Map.of("type", "text")),
                                entry("qclass", Map.of("type", "integer")),
                                entry("qclass_name", Map.of("type", "keyword")),
                                entry("qtype", Map.of("type", "integer")),
                                entry("qtype_name", Map.of("type", "keyword")),
                                entry("rcode", Map.of("type", "integer")),
                                entry("rcode_name", Map.of("type", "keyword")),
                                entry("AA", Map.of("type", "boolean")),
                                entry("TC", Map.of("type", "boolean")),
                                entry("RD", Map.of("type", "boolean")),
                                entry("RA", Map.of("type", "boolean")),
                                entry("Z", Map.of("type", "integer")),
                                entry("rejected", Map.of("type", "boolean"))
                        )
                ),
                transportClient
        );
    }


    private void manageIndex(String indexName, String docType, Map mappings, TransportClient transportClient) throws IOException {
        IndicesExistsResponse response = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();

        if (response.isExists()) {

            ImmutableOpenMap<String, MappingMetaData> index = transportClient.admin().indices().prepareGetMappings(indexName).get().getMappings().get(indexName);

            if (index.get(docType) == null || !mappings.equals(index.get(docType).getSourceAsMap())) {

                PutMappingRequest putMappingRequest = new PutMappingRequest(indexName).source(mappings).type(docType);
                transportClient.admin().indices().putMapping(putMappingRequest).actionGet();
            }


        } else {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).mapping(docType, mappings);
            CreateIndexResponse createIndexResponse = transportClient.admin().indices().create(createIndexRequest).actionGet();

            if (!createIndexResponse.isAcknowledged()) {
                return;
            }
        }
    }


    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {

        return new SinkWriter<T>() {

            private final RestClient restClient = RestClient.builder(
                            new HttpHost(
                                    host,
                                    port,
                                    scheme
                            )
                    )
                    .setRequestConfigCallback(
                            requestConfigBuilder -> requestConfigBuilder
                                    .setConnectTimeout(timeout)
                                    .setSocketTimeout(timeout)
                    )
                    .setMaxRetryTimeoutMillis(timeout)
                    .build();

            private final RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClient);

            @Override
            public void write(T element, Context context) throws IOException, InterruptedException {

                logger.info("Writing element: {}", element);

                restHighLevelClient
                        .indexAsync(
                                new IndexRequest(
                                        index,
                                        docType,
                                        UUID.randomUUID().toString()
                                )
                                        .source(element, XContentType.JSON)
                                ,
                                new ActionListener<IndexResponse>() {
                                    @Override
                                    public void onResponse(IndexResponse indexResponse) {
                                        logger.info("elasticsearch response {} ", indexResponse);
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        logger.error("Error writing element: {}", element, e);
                                    }
                                },
                                new BasicHeader("Content-Type", "application/json")
                        );
            }

            @Override
            public void flush(boolean endOfInput) throws IOException, InterruptedException {

            }

            @Override
            public void close() throws Exception {
                restClient.close();
            }
        };
    }
}
