package org.gbif.occurrence.query;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.registry.Dataset;
import org.gbif.utils.HttpUtil;
import org.gbif.ws.client.guice.GbifWsClientModule;

import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility ws-client class to get dataset and species titles used in downloads.
 */
public class TitleLookup {

  private static final int  HTTP_TIME_OUT =  2000;
  private static final Logger LOG = LoggerFactory.getLogger(TitleLookup.class);

  private final WebResource apiRoot;

  /**
   * Creates a lookup instance using a new http & jersey client.
   * This is a rather costly operation and should not be instantiated many times.
   * @param threads number of concurrent http client threads to use
   */
  public TitleLookup(String apiRootUrl, int threads) {
    apiRoot = GbifWsClientModule.buildJerseyClient(HttpUtil.newMultithreadedClient(HTTP_TIME_OUT, threads, threads))
      .resource(apiRootUrl);
  }

  /**
   * Creates a lookup instance from an existing jersey client resource pointing to the root of the API.
   */
  public TitleLookup(WebResource apiRoot) {
    this.apiRoot = apiRoot;
  }

  public String getDatasetTitle(String datasetKey) {
    try {
      return apiRoot.path("dataset").path(datasetKey).get(Dataset.class).getTitle();
    } catch (RuntimeException e) {
      LOG.error("Cannot lookup dataset title {}", datasetKey, e);
      return datasetKey;
    }
  }

  public String getSpeciesName(String usageKey) {
    try {
      return apiRoot.path("species").path(usageKey).get(NameUsage.class).getScientificName();
    } catch (RuntimeException e) {
      LOG.error("Cannot lookup species title {}", usageKey, e);
      return usageKey;
    }
  }

}
