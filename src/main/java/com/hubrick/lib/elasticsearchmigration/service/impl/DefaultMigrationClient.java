/**
 * Copyright (C) 2018 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.lib.elasticsearchmigration.service.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.hubrick.lib.elasticsearchmigration.exception.MigrationFailedException;
import com.hubrick.lib.elasticsearchmigration.exception.MigrationLockedException;
import com.hubrick.lib.elasticsearchmigration.exception.PreviousMigrationFailedException;
import com.hubrick.lib.elasticsearchmigration.model.es.LockEntry;
import com.hubrick.lib.elasticsearchmigration.model.es.LockEntryMeta;
import com.hubrick.lib.elasticsearchmigration.model.es.MigrationEntry;
import com.hubrick.lib.elasticsearchmigration.model.es.MigrationEntryMeta;
import com.hubrick.lib.elasticsearchmigration.model.es.State;
import com.hubrick.lib.elasticsearchmigration.model.migration.CreateIndexMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.IndexDocumentMigration;
import com.hubrick.lib.elasticsearchmigration.model.migration.Migration;
import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationMeta;
import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationSet;
import com.hubrick.lib.elasticsearchmigration.model.migration.MigrationSetEntry;
import com.hubrick.lib.elasticsearchmigration.model.migration.OpType;
import com.hubrick.lib.elasticsearchmigration.model.migration.UpdateDocumentMigration;
import com.hubrick.lib.elasticsearchmigration.service.MigrationClient;
import com.jayway.jsonpath.JsonPath;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@Slf4j
public class DefaultMigrationClient implements MigrationClient {

    private static final String WAIT_FOR_ACTIVE_SHARDS_FIELD = "wait_for_active_shards";

    static final String ELASTICSEARCH_MIGRATION_LOCK_INDEX;
    static final String ELASTICSEARCH_MIGRATION_VERSION_INDEX;

    static {
        try {
            ELASTICSEARCH_MIGRATION_LOCK_INDEX = Resources.toString(Resources.getResource(DefaultMigrationClient.class, "/schema/es/elasticsearch_migration_lock.json"), Charsets.UTF_8);
            ELASTICSEARCH_MIGRATION_VERSION_INDEX = Resources.toString(Resources.getResource(DefaultMigrationClient.class, "/schema/es/elasticsearch_migration_version.json"), Charsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load index files", e);
        }
    }

    private final String identifier;
    private final RestHighLevelClient restHighLevelClient;
    private final Boolean ignorePreviousFailures;
    private final Integer backoffPeriodInMillis;
    private final Integer retryCount;
    private final ObjectMapper objectMapper;

    private Integer numberOfNodesInCluster;
    private boolean init = false;
    private final AtomicInteger currentTry = new AtomicInteger(0);

    public DefaultMigrationClient(@NonNull final String identifier,
                                  @NonNull final RestHighLevelClient restHighLevelClient,
                                  @NonNull final Boolean ignorePreviousFailures,
                                  @NonNull final Integer backoffPeriodInMillis,
                                  @NonNull final Integer retryCount) {
        this.identifier = identifier;
        this.restHighLevelClient = restHighLevelClient;
        this.ignorePreviousFailures = ignorePreviousFailures;
        this.backoffPeriodInMillis = backoffPeriodInMillis;
        this.retryCount = retryCount;
        this.objectMapper = createObjectMapper();
    }

    private void init() {
        if (!init) {
            init = true;
            numberOfNodesInCluster = getNumberOfNodesInCluster();
            performRequestIgnoreExistingExceptions(new CreateIndexMigration(LockEntryMeta.INDEX, ELASTICSEARCH_MIGRATION_LOCK_INDEX));
            performRequestIgnoreExistingExceptions(new CreateIndexMigration(MigrationEntryMeta.INDEX, ELASTICSEARCH_MIGRATION_VERSION_INDEX));
        }
    }

    private ObjectMapper createObjectMapper() {
        final ObjectMapper objectMapper = new ObjectMapper();
        final JavaTimeModule javaTimeModule = new JavaTimeModule();

        javaTimeModule.addSerializer(Instant.class, new JsonSerializer<Instant>() {
            @Override
            public void serialize(Instant instant, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
                DateTimeFormatter formatter = DateTimeFormatter
                        .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                        .withZone(ZoneOffset.UTC);
                String serializedInstant = formatter.format(instant);
                jsonGenerator.writeString(serializedInstant);
            }
        });

        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        objectMapper.configure(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS, true);
        objectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        objectMapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(javaTimeModule);

        return objectMapper;
    }

    @Override
    public void applyMigrationSet(final MigrationSet migrationSet) {
        try {
            init();

            performUnderGlobalLock(() -> {
                final List<MigrationSetEntry> orderedMigrationSetEntries = Lists.newArrayList(migrationSet.getMigrations());
                orderedMigrationSetEntries.sort(Comparator.comparing(o -> o.getMigrationMeta().getVersion()));

                final List<MigrationEntry> allMigrations = getAllMigrations();
                log.info("Running checks...");
                checkAllPreviousMigrationsAppliedSuccessfully(allMigrations);
                checkForMetadataConflicts(allMigrations, orderedMigrationSetEntries.stream().map(e -> e.getMigrationMeta()).collect(Collectors.toList()));
                log.info("Checks done");

                final Set<String> appliedVersions = allMigrations.stream().map(e -> e.getVersion()).collect(Collectors.toSet());
                for (MigrationSetEntry migrationSetEntry : orderedMigrationSetEntries) {
                    log.info("Applying migration version " + migrationSetEntry.getMigrationMeta().getVersion());
                    if (appliedVersions.contains(migrationSetEntry.getMigrationMeta().getVersion())) {
                        log.info("Skipping migration. Already applied.");
                    } else {
                        try {
                            insertNewMigrationEntry(migrationSetEntry);
                            for (Migration migration : migrationSetEntry.getMigration()) {
                                log.info("Applying change " + migration.getClass().getSimpleName());
                                performRequest(migration);
                            }
                            updateMigrationEntry(migrationSetEntry.getMigrationMeta().getVersion(), State.SUCCESS, "");
                        } catch (Exception e) {
                            updateMigrationEntry(migrationSetEntry.getMigrationMeta().getVersion(), State.FAILURE, e.getCause().getMessage());
                            throw new MigrationFailedException("Performing migration version " + migrationSetEntry.getMigrationMeta().getVersion() + " failed. Message: " + e.getCause().getMessage(), e);
                        }
                    }
                }

                return null;
            });
        } catch (MigrationLockedException e) {
            if (currentTry.getAndIncrement() < retryCount) {
                try {
                    log.info("Migration locked. Retrying in {}ms", backoffPeriodInMillis);
                    Thread.sleep(backoffPeriodInMillis);
                    applyMigrationSet(migrationSet);
                } catch (InterruptedException e1) {
                    // Should never happen
                    throw new IllegalStateException("Error occured during backoff period.", e1);
                }
            } else {
                throw e;
            }
        }
    }

    private void insertNewMigrationEntry(MigrationSetEntry migrationSetEntry) throws JsonProcessingException {
        performRequest(
                new IndexDocumentMigration(
                        MigrationEntryMeta.INDEX,
                        MigrationEntryMeta.TYPE,
                        Optional.of(identifier + "-" + migrationSetEntry.getMigrationMeta().getVersion()),
                        Optional.of(OpType.CREATE),
                        objectMapper.writeValueAsString(
                                new MigrationEntry(
                                        identifier,
                                        migrationSetEntry.getMigrationMeta().getVersion(),
                                        migrationSetEntry.getMigrationMeta().getName(),
                                        migrationSetEntry.getMigrationMeta().getSha256Checksums(),
                                        State.IN_PROGRESS,
                                        null,
                                        Instant.now()
                                )
                        )
                )
        );
    }

    private void updateMigrationEntry(String version, State state, String failureMessage) {
        final Map<String, Map<String, String>> update = ImmutableMap.of(
                "doc",
                ImmutableMap.of(
                        MigrationEntryMeta.STATE_FIELD, state.name(),
                        MigrationEntryMeta.FAUILURE_MESSAGE_FIELD, failureMessage
                )
        );

        try {
            performRequest(
                    new UpdateDocumentMigration(
                            MigrationEntryMeta.INDEX,
                            MigrationEntryMeta.TYPE,
                            identifier + "-" + version,
                            objectMapper.writeValueAsString(update)
                    )
            );
        } catch (Exception e) {
            throw new MigrationFailedException("Performing migration version " + version + " failed. Message: " + e.getCause().getMessage());
        }
    }

    private void checkAllPreviousMigrationsAppliedSuccessfully(final List<MigrationEntry> migrationEntries) {
        if (!ignorePreviousFailures) {
            for (MigrationEntry migrationEntry : migrationEntries) {
                if (migrationEntry.getState() != State.SUCCESS) {
                    throw new PreviousMigrationFailedException("Previous migration in FAILED state. Message: " + migrationEntry.getFailureMessage());
                }
            }
        }
    }

    private void checkForMetadataConflicts(final List<MigrationEntry> migrationEntries, final List<MigrationMeta> migrationMetas) {
        if (migrationMetas.size() < migrationEntries.stream().filter(e -> State.SUCCESS == e.getState()).count()) {
            throw new MigrationFailedException("Local migration set smaller then one found in ES. Local migration set: " + migrationMetas.size() + ", ES migration set: " + migrationEntries.size());
        }

        for (int i = 0; i < migrationEntries.size(); i++) {
            if (!migrationEntries.get(i).getVersion().equals(migrationMetas.get(i).getVersion())) {
                throw new MigrationFailedException("Version mismatch for " + migrationEntries.get(i).getName() + ". Local version: " + migrationMetas.get(i).getVersion() + ", ES version: " + migrationEntries.get(i).getVersion());
            } else if (Sets.intersection(migrationEntries.get(i).getSha256Checksum(), migrationMetas.get(i).getSha256Checksums()).isEmpty()) {
                throw new MigrationFailedException("Checksum mismatch for " + migrationEntries.get(i).getName() + ". Local checksums: " + migrationMetas.get(i).getVersion() + ":" + migrationMetas.get(i).getSha256Checksums() + ", ES checksums: " + migrationEntries.get(i).getVersion() + ":" + migrationEntries.get(i).getSha256Checksum());
            } else if (!migrationEntries.get(i).getName().equals(migrationMetas.get(i).getName())) {
                throw new MigrationFailedException("Name mismatch. Local checksum: " + migrationMetas.get(i).getVersion() + ":" + migrationMetas.get(i).getName() + ", ES checksum: " + migrationEntries.get(i).getVersion() + ":" + migrationEntries.get(i).getName());
            }
        }

        // Should never happen since the changeset is ordered by version but better safe then sorry
        final Optional<MigrationEntry> lastMigrationEntry = Optional.ofNullable(Iterables.getLast(migrationEntries, null));
        for (int i = migrationEntries.size(); i < migrationMetas.size(); i++) {
            if (lastMigrationEntry.isPresent() && lastMigrationEntry.get().getVersion().compareTo(migrationMetas.get(i).getVersion()) >= 0) {
                throw new MigrationFailedException("Migration Set contains version lower or equal to the latest applied version. New version: " + migrationMetas.get(i).getVersion() + ", Latest applied version: " + lastMigrationEntry.get().getVersion());
            }
        }
    }

    public void performRequestIgnoreExistingExceptions(final Migration migration) {
        try {
            performRequest(migration);
        } catch (MigrationFailedException e) {
            if (e.getCause() instanceof ResponseException) {
                final ResponseException responseException = (ResponseException) e.getCause();
                if (responseException.getResponse().getStatusLine().getStatusCode() == 400 &&
                        (responseException.getMessage().contains("index_already_exists_exception") || // ES 5.x
                                responseException.getMessage().contains("resource_already_exists_exception") || // ES 6.x
                                responseException.getMessage().contains("IndexAlreadyExistsException"))) { // ES 1.x and 2.x
                    return;
                }
            }

            throw e;
        }
    }

    private List<MigrationEntry> getAllMigrations() {
        try {
            final QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery(MigrationEntryMeta.IDENTIFIER_FIELD, identifier));
            final SearchRequest searchRequest = new SearchRequest()
                    .indices(MigrationEntryMeta.INDEX)
                    .searchType(SearchType.DEFAULT)
                    .source(SearchSourceBuilder.searchSource().query(queryBuilder).fetchSource(true).size(1000).sort(MigrationEntryMeta.VERSION_FIELD, SortOrder.ASC));

            final SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
            if (searchResponse.status() == RestStatus.OK) {
                return transformHitsFromEs(searchResponse.getHits(), MigrationEntry.class);
            } else {
                throw new MigrationFailedException("Could not access '" + MigrationEntryMeta.INDEX + "' index. Failures: " + Arrays.asList(searchResponse.getShardFailures()));
            }
        } catch (IOException e) {
            throw new MigrationFailedException("IO Exception during migration", e);
        }
    }

    public void performRequest(final Migration migration) {
        try {
            final StringEntity stringEntity = new StringEntity(migration.getBody(), ContentType.APPLICATION_JSON);
            restHighLevelClient.getLowLevelClient().performRequest(migration.getMethod().name(), migration.getUrl(), augmentParameters(migration.getParameters()), stringEntity, convertToHeaderArray(migration.getHeaders()));
        } catch (ResponseException e) {
            throw new MigrationFailedException("Error performing migration", e);
        } catch (IOException e) {
            throw new MigrationFailedException("IO Exception during migration", e);
        }
    }

    private Header[] convertToHeaderArray(Multimap<String, String> headers) {
        return headers.entries().stream().map(e -> new BasicHeader(e.getKey(), e.getValue())).collect(Collectors.toSet()).toArray(new Header[0]);
    }


    private Map<String, String> augmentParameters(Map<String, String> originalParameters) {
        final Map<String, String> augmentedParameters = new HashMap<>(originalParameters);
        augmentedParameters.remove(WAIT_FOR_ACTIVE_SHARDS_FIELD);

        return originalParameters.containsKey(WAIT_FOR_ACTIVE_SHARDS_FIELD) ?
                ImmutableMap.<String, String>builder()
                        .putAll(augmentedParameters)
                        .put(WAIT_FOR_ACTIVE_SHARDS_FIELD, String.valueOf(Math.min(Integer.valueOf(originalParameters.get(WAIT_FOR_ACTIVE_SHARDS_FIELD)), numberOfNodesInCluster)))
                        .build() :
                augmentedParameters;
    }


    public int getNumberOfNodesInCluster() {
        try {
            final Response response = restHighLevelClient.getLowLevelClient().performRequest("GET", "/_nodes");
            return JsonPath.read(IOUtils.toString(response.getEntity().getContent(), Charsets.UTF_8), "$._nodes.total");
        } catch (ResponseException e) {
            throw new MigrationFailedException("Error performing migration", e);
        } catch (IOException e) {
            throw new MigrationFailedException("IO Exception during migration", e);
        }
    }

    public int getNumberOfShards(String index) {
        try {
            final Response response = restHighLevelClient.getLowLevelClient().performRequest("GET", "/" + index + "_settings");
            return JsonPath.read(IOUtils.toString(response.getEntity().getContent(), Charsets.UTF_8), "$." + index + ".settings.index.number_of_shards");
        } catch (ResponseException e) {
            throw new MigrationFailedException("Error performing migration", e);
        } catch (IOException e) {
            throw new MigrationFailedException("IO Exception during migration", e);
        }
    }


    private <T> T performUnderGlobalLock(Action0<T> action) {
        if (acquireGlobalLock()) {
            try {
                return action.call();
            } finally {
                releaseGlobalLock();
            }
        } else {
            throw new MigrationLockedException("Migration is locked by another process");
        }
    }

    private boolean acquireGlobalLock() {
        try {
            final IndexRequest indexRequest = new IndexRequest().index(LockEntryMeta.INDEX)
                    .create(true)
                    .id(identifier + "-global")
                    .type(LockEntryMeta.TYPE)
                    .source(objectMapper.writeValueAsString(new LockEntry(Instant.now())), XContentType.JSON);
            restHighLevelClient.index(indexRequest);
            return true;
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.CONFLICT &&
                    (e.getMessage().contains("version_conflict_engine_exception"))) {  // ES 6.x
                return false;
            }

            throw new MigrationFailedException("Error acquiring lock", e);
        } catch (IOException e) {
            throw new MigrationFailedException("IO Exception during migration", e);
        }
    }

    private boolean releaseGlobalLock() {
        try {
            final DeleteRequest deleteRequest = new DeleteRequest().index(LockEntryMeta.INDEX).id(identifier + "-global").type(LockEntryMeta.TYPE);
            restHighLevelClient.delete(deleteRequest);
            return true;
        } catch (IOException e) {
            throw new MigrationFailedException("IO Exception during migration", e);
        }
    }

    private <T> List<T> transformHitsFromEs(SearchHits hits, Class<T> clazz) {
        return Arrays.asList(hits.getHits()).stream()
                .map(hit -> transformSourceFromEs(hit.getSourceAsString(), clazz))
                .collect(Collectors.toList());
    }

    private <T> T transformSourceFromEs(String source, Class<T> clazz) {
        try {
            if (source != null) {
                log.debug("Response from ES: {}", source);
                return objectMapper.readValue(source, clazz);
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface Action0<T> {
        T call();
    }
}
