package com.linkedin.metadata.search;

import com.datahub.test.Snapshot;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.TestEntityUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.ElasticSearchTestConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.CachingAllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.util.Collections;

import static com.linkedin.metadata.ElasticSearchTestConfiguration.syncAfterWrite;
import static org.testng.Assert.assertEquals;

@Import(ElasticSearchTestConfiguration.class)
public class SearchServiceTest extends AbstractTestNGSpringContextTests {

  @Autowired
  private RestHighLevelClient _searchClient;
  @Autowired
  private ESBulkProcessor _bulkProcessor;
  @Autowired
  private ESIndexBuilder _esIndexBuilder;
  private EntityRegistry _entityRegistry;
  private IndexConvention _indexConvention;
  private SettingsBuilder _settingsBuilder;
  private ElasticSearchService _elasticSearchService;
  private CacheManager _cacheManager;
  private SearchService _searchService;

  private static final String ENTITY_NAME = "testEntity";

  @BeforeClass
  public void setup() {
    _entityRegistry = new SnapshotEntityRegistry(new Snapshot());
    _indexConvention = new IndexConventionImpl("search_service_test");
    _settingsBuilder = new SettingsBuilder(Collections.emptyList(), null);
    _elasticSearchService = buildEntitySearchService();
    _elasticSearchService.configure();
    _cacheManager = new ConcurrentMapCacheManager();
    resetSearchService();
  }

  private void resetSearchService() {
    CachingEntitySearchService cachingEntitySearchService = new CachingEntitySearchService(
        _cacheManager,
        _elasticSearchService,
        100,
        true);
    _searchService = new SearchService(
      new EntityDocCountCache(_entityRegistry, _elasticSearchService),
      cachingEntitySearchService,
      new CachingAllEntitiesSearchAggregator(
          _cacheManager,
          new AllEntitiesSearchAggregator(
              _entityRegistry,
              _elasticSearchService,
              cachingEntitySearchService,
              new SimpleRanker()),
          100,
          true),
      new SimpleRanker());
  }

  @BeforeMethod
  public void wipe() throws Exception {
    _elasticSearchService.clear();
    syncAfterWrite();
  }

  @Nonnull
  private ElasticSearchService buildEntitySearchService() {
    EntityIndexBuilders indexBuilders =
        new EntityIndexBuilders(_esIndexBuilder, _entityRegistry,
            _indexConvention, _settingsBuilder);
    ESSearchDAO searchDAO = new ESSearchDAO(_entityRegistry, _searchClient, _indexConvention);
    ESBrowseDAO browseDAO = new ESBrowseDAO(_entityRegistry, _searchClient, _indexConvention);
    ESWriteDAO writeDAO = new ESWriteDAO(_entityRegistry, _searchClient, _indexConvention,
        _bulkProcessor, 1);
    return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
  }

  private void clearCache() {
    _cacheManager.getCacheNames().forEach(cache -> _cacheManager.getCache(cache).clear());
    resetSearchService();
  }

  @Test
  public void testSearchService() throws Exception {
    SearchResult searchResult =
        _searchService.searchAcrossEntities(ImmutableList.of(ENTITY_NAME), "test", null, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());
    syncAfterWrite();

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();

    Urn urn2 = new TestEntityUrn("test", "testUrn2", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("random"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride2"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/b/c"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());
    syncAfterWrite();

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();

    _elasticSearchService.deleteDocument(ENTITY_NAME, urn.toString());
    _elasticSearchService.deleteDocument(ENTITY_NAME, urn2.toString());
    syncAfterWrite();
    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", null, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 0);
  }

  @Test
  public void testAdvancedSearchOr() throws Exception {
    final Criterion filterCriterion =  new Criterion()
        .setField("platform")
        .setCondition(Condition.EQUAL)
        .setValue("hive")
        .setValues(new StringArray(ImmutableList.of("hive")));

    final Criterion subtypeCriterion =  new Criterion()
        .setField("subtypes")
        .setCondition(Condition.EQUAL)
        .setValue("")
        .setValues(new StringArray(ImmutableList.of("view")));

    final Filter filterWithCondition = new Filter().setOr(
        new ConjunctiveCriterionArray(
            new ConjunctiveCriterion().setAnd(
                new CriterionArray(ImmutableList.of(filterCriterion))),
            new ConjunctiveCriterion().setAnd(
                new CriterionArray(ImmutableList.of(subtypeCriterion)))
        ));


    SearchResult searchResult =
        _searchService.searchAcrossEntities(ImmutableList.of(ENTITY_NAME), "test", filterWithCondition, null, 0, 10, null);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("subtypes", JsonNodeFactory.instance.textNode("view"));
    document.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());

    Urn urn2 = new TestEntityUrn("test", "testUrn", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document2.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document2.set("platform", JsonNodeFactory.instance.textNode("hive"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("test", "testUrn", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document3.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document3.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document3.toString(), urn3.toString());

    syncAfterWrite();

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", filterWithCondition, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 2);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    assertEquals(searchResult.getEntities().get(1).getEntity(), urn2);
    clearCache();
  }

  @Test
  public void testAdvancedSearchSoftDelete() throws Exception {
    final Criterion filterCriterion =  new Criterion()
        .setField("platform")
        .setCondition(Condition.EQUAL)
        .setValue("hive")
        .setValues(new StringArray(ImmutableList.of("hive")));

    final Criterion removedCriterion =  new Criterion()
        .setField("removed")
        .setCondition(Condition.EQUAL)
        .setValue("")
        .setValues(new StringArray(ImmutableList.of("true")));

    final Filter filterWithCondition = new Filter().setOr(
        new ConjunctiveCriterionArray(
            new ConjunctiveCriterion().setAnd(
                new CriterionArray(ImmutableList.of(filterCriterion, removedCriterion)))
        ));


    SearchResult searchResult =
        _searchService.searchAcrossEntities(ImmutableList.of(ENTITY_NAME), "test", filterWithCondition, null, 0, 10, null);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("subtypes", JsonNodeFactory.instance.textNode("view"));
    document.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(true));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());

    Urn urn2 = new TestEntityUrn("test", "testUrn", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document2.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document2.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("test", "testUrn", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document3.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document3.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document3.toString(), urn3.toString());

    syncAfterWrite();

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", filterWithCondition, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn);
    clearCache();
  }

  @Test
  public void testAdvancedSearchNegated() throws Exception {
    final Criterion filterCriterion =  new Criterion()
        .setField("platform")
        .setCondition(Condition.EQUAL)
        .setValue("hive")
        .setNegated(true)
        .setValues(new StringArray(ImmutableList.of("hive")));

    final Filter filterWithCondition = new Filter().setOr(
        new ConjunctiveCriterionArray(
            new ConjunctiveCriterion().setAnd(
                new CriterionArray(ImmutableList.of(filterCriterion)))
        ));


    SearchResult searchResult =
        _searchService.searchAcrossEntities(ImmutableList.of(ENTITY_NAME), "test", filterWithCondition, null, 0, 10, null);

    assertEquals(searchResult.getNumEntities().intValue(), 0);
    clearCache();

    Urn urn = new TestEntityUrn("test", "testUrn", "VALUE_1");
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.set("urn", JsonNodeFactory.instance.textNode(urn.toString()));
    document.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document.set("subtypes", JsonNodeFactory.instance.textNode("view"));
    document.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(true));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document.toString(), urn.toString());

    Urn urn2 = new TestEntityUrn("test", "testUrn", "VALUE_2");
    ObjectNode document2 = JsonNodeFactory.instance.objectNode();
    document2.set("urn", JsonNodeFactory.instance.textNode(urn2.toString()));
    document2.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document2.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document2.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document2.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document2.set("platform", JsonNodeFactory.instance.textNode("hive"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document2.toString(), urn2.toString());

    Urn urn3 = new TestEntityUrn("test", "testUrn", "VALUE_3");
    ObjectNode document3 = JsonNodeFactory.instance.objectNode();
    document3.set("urn", JsonNodeFactory.instance.textNode(urn3.toString()));
    document3.set("keyPart1", JsonNodeFactory.instance.textNode("test"));
    document3.set("textFieldOverride", JsonNodeFactory.instance.textNode("textFieldOverride"));
    document3.set("browsePaths", JsonNodeFactory.instance.textNode("/a/b/c"));
    document3.set("subtypes", JsonNodeFactory.instance.textNode("table"));
    document3.set("platform", JsonNodeFactory.instance.textNode("snowflake"));
    document.set("removed", JsonNodeFactory.instance.booleanNode(false));
    _elasticSearchService.upsertDocument(ENTITY_NAME, document3.toString(), urn3.toString());

    syncAfterWrite();

    searchResult = _searchService.searchAcrossEntities(ImmutableList.of(), "test", filterWithCondition, null, 0, 10, null);
    assertEquals(searchResult.getNumEntities().intValue(), 1);
    assertEquals(searchResult.getEntities().get(0).getEntity(), urn3);
    clearCache();
  }
}
