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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.registry.Dataset;

import org.gbif.api.ws.mixin.Mixins;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.WebTarget;
import java.net.URL;

/**
 * Utility ws-client class to get dataset and species titles used in downloads.
 */
public class TitleLookupServiceImpl implements TitleLookupService {

  private static final Logger LOG = LoggerFactory.getLogger(TitleLookupServiceImpl.class);

  private final String apiRoot;
  final ObjectMapper objectMapper;

  /**
   * Creates a lookup instance from an existing jersey client resource pointing to the root of the API.
   */
  public TitleLookupServiceImpl(String apiRoot) {
    this.apiRoot = apiRoot;
    objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    Mixins.getPredefinedMixins().forEach(objectMapper::addMixIn);
  }

  @Override
  public String getDatasetTitle(String datasetKey) {
    String apiUrl = apiRoot + "dataset/" + datasetKey;
    try {
      URL url = new URL(apiUrl);
      Dataset dataset = objectMapper.readValue(url, Dataset.class);
      return dataset.getTitle();
    } catch (Exception e) {
      LOG.error("Cannot lookup dataset title {}", datasetKey, e);
    }
    return datasetKey;
  }

  @Override
  public String getSpeciesName(String usageKey) {
    try {
      String apiUrl = apiRoot + "species/" + usageKey;
      URL url = new URL(apiUrl);
      NameUsage nameUsage = objectMapper.readValue(url, NameUsage.class);
      return nameUsage.getScientificName();
    } catch (Exception e) {
      LOG.error("Cannot lookup species title {}", usageKey, e);
    }
    return usageKey;
  }

  @Override
  public String getSpeciesName(String usageKey, String checklistKey) {
    try {
      String apiUrl = apiRoot + "species/" + usageKey;
      URL url = new URL(apiUrl);
      NameUsage nameUsage = objectMapper.readValue(url, NameUsage.class);
      return nameUsage.getScientificName();
    } catch (Exception e) {
      LOG.error("Cannot lookup species title {}", usageKey, e);
    }
    return usageKey;
  }
}
