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

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.registry.Dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.WebResource;

/**
 * Utility ws-client class to get dataset and species titles used in downloads.
 */
public class TitleLookupServiceImpl implements TitleLookupService {

  private static final Logger LOG = LoggerFactory.getLogger(TitleLookupServiceImpl.class);

  private final WebResource apiRoot;

  /**
   * Creates a lookup instance from an existing jersey client resource pointing to the root of the API.
   */
  public TitleLookupServiceImpl(WebResource apiRoot) {
    this.apiRoot = apiRoot;
  }

  @Override
  public String getDatasetTitle(String datasetKey) {
    try {
      return apiRoot.path("dataset").path(datasetKey).get(Dataset.class).getTitle();
    } catch (RuntimeException e) {
      LOG.error("Cannot lookup dataset title {}", datasetKey, e);
      return datasetKey;
    }
  }

  @Override
  public String getSpeciesName(String usageKey) {
    try {
      return apiRoot.path("species").path(usageKey).get(NameUsage.class).getScientificName();
    } catch (RuntimeException e) {
      LOG.error("Cannot lookup species title {}", usageKey, e);
      return usageKey;
    }
  }
}
