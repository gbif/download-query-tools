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
import com.fasterxml.jackson.databind.DeserializationContext;
import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.common.DOI;
import org.gbif.api.model.registry.Dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;

import java.lang.reflect.Type;

/**
 * Utility ws-client class to get dataset and species titles used in downloads.
 */
public class TitleLookupServiceImpl implements TitleLookupService {

  private static final Logger LOG = LoggerFactory.getLogger(TitleLookupServiceImpl.class);

  private final WebTarget apiRoot;

  /**
   * Creates a lookup instance from an existing jersey client resource pointing to the root of the API.
   */
  public TitleLookupServiceImpl(WebTarget apiRoot) {
    this.apiRoot = apiRoot;
  }

  @Override
  public String getDatasetTitle(String datasetKey) {
    try {
      WebTarget target = apiRoot.path("dataset").path(datasetKey);
      Response response = target.request().get();

      if (response.getStatus() == 200) {
        Dataset dataset = response.readEntity(Dataset.class);
        return dataset.getTitle();
      } else {
        LOG.error("Failed to retrieve dataset title for {}. Status code: {}", datasetKey, response.getStatus());
      }
    } catch (RuntimeException e) {
      LOG.error("Cannot lookup dataset title {}", datasetKey, e);
    }
    return datasetKey;
  }

  @Override
  public String getSpeciesName(String usageKey) {
    try {
      WebTarget target = apiRoot.path("species").path(usageKey);
      Response response = target.request().get();

      if (response.getStatus() == 200) {
        NameUsage nameUsage = response.readEntity(NameUsage.class);
        return nameUsage.getScientificName();
      } else {
        LOG.error("Failed to retrieve species name for {}. Status code: {}", usageKey, response.getStatus());
      }
    } catch (RuntimeException e) {
      LOG.error("Cannot lookup species title {}", usageKey, e);
    }
    return usageKey;
  }

//  public class DOIDeserializer implements jakarta.json.bind.serializer.JsonbDeserializer<DOI> {
//    @Override
//    public DOI deserialize(jakarta.json.stream.JsonParser jsonParser, DeserializationContext deserializationContext, Type type) {
//      if (jsonParser.next() == jakarta.json.stream.JsonParser.Event.VALUE_STRING) {
//        String doiValue = jsonParser.getString();
//        return new DOI(doiValue);  // assuming DOI has a constructor that accepts a string
//      } else {
//        throw new JsonbException("Expected DOI as a string.");
//      }
//    }
//  }
}
