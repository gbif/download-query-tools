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

import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HumanPredicateBuilderTest {

  private HumanPredicateBuilder builder;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  public void init() {
    TitleLookupServiceImpl tl = mock(TitleLookupServiceImpl.class);
    when(tl.getDatasetTitle(ArgumentMatchers.any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(ArgumentMatchers.any())).thenReturn("Abies alba Mill.");
    builder = new HumanPredicateBuilder(tl);
  }

  @Test
  public void testHumanPredicateStringNull() {
    String s = null;
    Predicate p = null;
    builder.humanFilter(p);
    assertEquals("{ }", builder.humanFilterString(p));
    assertEquals("{ }", builder.humanFilterString(s));
  }

  @Test
  public void humanPredicateFilterTest() throws Exception {
    String expected =
        new String(
            Files.readAllBytes(
                Paths.get(getClass().getClassLoader().getResource("result.txt").getPath())),
            StandardCharsets.UTF_8);
    try (Stream<String> stream =
        Files.lines(Paths.get(getClass().getClassLoader().getResource("source.txt").getPath()))) {
      String actual =
          stream
              .map(p -> p.substring(1, p.length() - 1))
              .map(
                  stringPredicate -> {
                    try {
                      mapper
                          .writerWithDefaultPrettyPrinter()
                          .writeValueAsString(mapper.readTree(stringPredicate));
                      return builder.humanFilterString(stringPredicate);
                    } catch (Exception e) {
                      System.out.println("there is an exception");
                      return null;
                    }
                  })
              .collect(Collectors.joining("\n---------------------------------------------\n"));

      assertEquals(expected.trim(), actual.trim());
    }
  }

  @Test
  public void testTooManyLookups() {
    // If there are more than 10,050 lookups (dataset, taxa) give up; it's likely to be too slow.

    List<String> bigList = new ArrayList<>();
    for (int i = 0; i < 11000; i++) {
      bigList.add("" + i);
    }
    Predicate bigIn = new InPredicate(OccurrenceSearchParameter.TAXON_KEY, bigList, false);

    try {
      builder.humanFilter(bigIn);
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      builder.humanFilterString(bigIn);
      fail();
    } catch (IllegalStateException e) {
    }
  }
}
