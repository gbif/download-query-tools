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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TitleLookupTest {

  @Test
  public void lookupDatasetTitleTest() {
    TitleLookupService service =
        TitleLookupServiceFactory.getInstance("http://api.gbif-dev.org/v1/");

    assertEquals(
        "FBIP: Identification of viruses infecting indigenous ornamental bulbous plants in South Africa using NGS",
        service.getDatasetTitle("25be374b-8318-4a8c-ae3c-7ae592233afb"));
    assertEquals(
        "FBIP: Mycorrhizal associations of Erica hair roots in South African fynbos",
        service.getDatasetTitle("417276eb-5082-4da3-b410-fe5680da65d9"));
    assertEquals(
        "FBIP: plantae_magnoliophyta_collection",
        service.getDatasetTitle("7abad3fc-068a-4f34-8b3d-bfe03155ccde"));
  }

  @Test
  public void lookupSpeciesNameTest() {
    TitleLookupService service =
        TitleLookupServiceFactory.getInstance("http://api.gbif-dev.org/v1/");

    assertEquals("incertae sedis", service.getSpeciesName("0"));
    assertEquals("Animalia", service.getSpeciesName("1"));
    assertEquals("Archaea", service.getSpeciesName("2"));
  }
}
