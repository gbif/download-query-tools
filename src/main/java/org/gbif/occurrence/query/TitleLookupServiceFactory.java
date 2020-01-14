package org.gbif.occurrence.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.gbif.api.ws.mixin.Mixins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public final class TitleLookupServiceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TitleLookupServiceFactory.class);

  private TitleLookupServiceFactory() {
  }

  public static TitleLookupService getInstance(String apiRootProperty) {
    URI apiRoot = URI.create(Preconditions.checkNotNull(apiRootProperty, "API url can't be null"));
    Preconditions.checkArgument(apiRoot.getHost() != null, "API url must have a host! " + apiRoot);
    LOG.info("Create new TitleLookup using API {}", apiRoot);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    Mixins.getPredefinedMixins().forEach(objectMapper::addMixIn);

    ClientConfig realClientConfig = new DefaultClientConfig();
    realClientConfig.getSingletons().add(new JacksonJsonProvider(objectMapper));

    Client client = Client.create(realClientConfig);
    WebResource api = client.resource(apiRoot);

    return new TitleLookupServiceImpl(api);
  }
}
