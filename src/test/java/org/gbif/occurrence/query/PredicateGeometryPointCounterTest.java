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

import org.gbif.api.model.occurrence.predicate.*;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;

import java.util.*;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PredicateGeometryPointCounterTest {
  PredicateGeometryPointCounter counter = new PredicateGeometryPointCounter();

  /**
   * test all available search parameters and make sure we have a bundle entry for all possible enum values
   */
  @Test
  public void testAllParams() {

    final String date = DateFormatUtils.ISO_8601_EXTENDED_DATE_FORMAT.format(new Date());
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
        } else if (p == OccurrenceSearchParameter.GEO_DISTANCE) {
          ands.add(new GeoDistancePredicate("90", "100", "5km"));
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
    assertEquals(5, c);
  }

  @Test
  public void testPolygons() {
    int c =
        counter.count(
            new DisjunctionPredicate(
                Arrays.asList(
                    new WithinPredicate("POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))"),
                    new WithinPredicate("POLYGON ((30 10, 10 20, 20 40, 30 10))"))));
    assertEquals(9, c);
  }

  @Test
  public void testCountNull() {
    assertEquals(0, (int) counter.count(null));
  }
}
