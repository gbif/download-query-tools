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

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;
import org.gbif.api.ws.mixin.Mixins;
import java.net.URI;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import org.glassfish.jersey.client.ClientConfig;

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

    JacksonJsonProvider jacksonJsonProvider = new JacksonJsonProvider(objectMapper);
    ClientConfig clientConfig = new ClientConfig();
    clientConfig.register(jacksonJsonProvider);  // Register Jackson provider with Jersey client

    Client client = ClientBuilder.newClient(clientConfig);
    WebTarget api = client.target(apiRoot);

    return new TitleLookupServiceImpl(api);
  }
}
