package org.gbif.occurrence.query;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.registry.Dataset;
import org.gbif.utils.HttpUtil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import org.apache.http.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TitleLookup {
  private static final Logger LOG = LoggerFactory.getLogger(TitleLookup.class);
  private final WebResource apiRoot;

  /**
   * Creates a lookup instance using a new http & jersey client.
   * This is a rather costly operation and should not be instantiated many times.
   */
  public TitleLookup(String apiRootUrl) {
    this.apiRoot = provideJerseyClient().resource(apiRootUrl);
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

  private Client provideJerseyClient() {
    HttpClient client = HttpUtil.newMultithreadedClient(2000, 4, 4);
    ApacheHttpClient4Handler hch = new ApacheHttpClient4Handler(client, null, false);

    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    return new ApacheHttpClient4(hch, clientConfig);
  }
}
