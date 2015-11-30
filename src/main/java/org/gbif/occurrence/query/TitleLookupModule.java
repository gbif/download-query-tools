package org.gbif.occurrence.query;

import org.gbif.utils.HttpUtil;
import org.gbif.ws.json.JacksonJsonContextResolver;

import java.net.URI;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import org.apache.http.client.HttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guice module providing a TitleLookup instance for the HumanFilterBuilder to lookup species and dataset titles.
 */
public class TitleLookupModule extends PrivateModule {
  private static final Logger LOG = LoggerFactory.getLogger(TitleLookupModule.class);
  private final boolean provideHttpClient;
  private final URI apiRoot;

  /**
   * @param provideHttpClient if true the module creates a new internal only http client instance
   */
  public TitleLookupModule(boolean provideHttpClient, String apiRoot) {
    this.provideHttpClient = provideHttpClient;
    this.apiRoot = URI.create(Preconditions.checkNotNull(apiRoot, "API url can't be null"));
    Preconditions.checkArgument(this.apiRoot.getHost()!=null, "API url must have a host! " + apiRoot);
    LOG.info("Create new TitleLookup using API " + apiRoot);
  }

  @Override
  protected void configure() {
    if (provideHttpClient) {
      bind(HttpClient.class).toInstance(provideHttpClient());
    }
    expose(TitleLookup.class);
  }

  @Provides
  @Singleton
  @Inject
  public TitleLookup provideLookup(HttpClient hc) {
    ApacheHttpClient4Handler hch = new ApacheHttpClient4Handler(hc, null, false);
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getClasses().add(JacksonJsonContextResolver.class);
    // this line is critical! Note that this is the jersey version of this class name!
    clientConfig.getClasses().add(JacksonJsonProvider.class);
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    WebResource api = new ApacheHttpClient4(hch, clientConfig).resource(apiRoot);
    return new TitleLookup(api);
  }

  private static HttpClient provideHttpClient() {
    return HttpUtil.newMultithreadedClient(5000, 10, 10);
  }

}
