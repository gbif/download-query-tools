/*
 * Copyright 2021 Global Biodiversity Information Facility (GBIF)
 *
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

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PredicateCounterTest {
  PredicateCounter counter = new PredicateCounter();

  /**
   * test all available search parameters and make sure we have a bundle entry for all possible enum values
   */
  @Test
  public void testAllParams() {

    final String date = DateFormatUtils.ISO_DATE_FORMAT.format(new Date());
    List<Predicate> ands = new ArrayList<>();
    for (OccurrenceSearchParameter p : OccurrenceSearchParameter.values()) {
      if (p.type().isEnum()) {
        if (p.type() == Country.class) {
          ands.add(new EqualsPredicate(p, Country.DENMARK.getIso2LetterCode(), false));

        } else if (p.type() == Continent.class) {
          ands.add(new EqualsPredicate(p, Continent.AFRICA.getTitle(), false));

        } else {
          Class<Enum<?>> vocab = (Class<Enum<?>>) p.type();
          // add a comparison for every possible enum value to test the resource bundle for
          // completeness
          List<Predicate> ors = new ArrayList<>();
          for (Enum<?> e : vocab.getEnumConstants()) {
            ors.add(new EqualsPredicate(p, e.toString(), false));
          }
          ands.add(new DisjunctionPredicate(ors));
        }

      } else if (p.type() == Date.class) {
        ands.add(new EqualsPredicate(p, date, false));

      } else if (p.type() == Double.class) {
        ands.add(new EqualsPredicate(p, "12.478", false));

      } else if (p.type() == Integer.class) {
        ands.add(new EqualsPredicate(p, "10", false));

      } else if (p.type() == String.class) {
        if (p == OccurrenceSearchParameter.GEOMETRY) {
          ands.add(new WithinPredicate("POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))"));
        } else {
          ands.add(new EqualsPredicate(p, "Bernd Neumann", false));
        }

      } else if (p.type() == Boolean.class) {
        ands.add(new EqualsPredicate(p, "true", false));

      } else if (p.type() == UUID.class) {
        ands.add(new EqualsPredicate(p, UUID.randomUUID().toString(), false));

      } else {
        throw new IllegalStateException("Unknown SearchParameter type " + p.type());
      }
    }
    ConjunctionPredicate and = new ConjunctionPredicate(ands);

    int c = counter.count(and);
    assertEquals(227, c);
  }

  @Test
  public void testTaxa() {
    int c =
        counter.count(
            new InPredicate(
                OccurrenceSearchParameter.TAXON_KEY, Arrays.asList("1", "2", "3"), false));

    assertEquals(3, c);
  }

  @Test
  public void testCountNull() {
    assertEquals(0, (int) counter.count(null));
  }
}
