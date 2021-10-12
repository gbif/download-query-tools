/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.query;

import org.gbif.api.ws.mixin.Mixins;

import java.net.URI;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

import static org.gbif.api.util.PreconditionUtils.checkArgument;

public final class TitleLookupServiceFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TitleLookupServiceFactory.class);

  private TitleLookupServiceFactory() {}

  public static TitleLookupService getInstance(String apiRootProperty) {
    URI apiRoot = URI.create(Objects.requireNonNull(apiRootProperty, "API url can't be null"));
    checkArgument(apiRoot.getHost() != null, "API url must have a host! " + apiRoot);
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
