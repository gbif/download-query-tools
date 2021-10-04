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
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.vocabulary.Country;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryParameterFilterBuilderTest {

  @Test
  public void testMultiValues() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();

    Predicate p =
        new EqualsPredicate(
            OccurrenceSearchParameter.COUNTRY, Country.AFGHANISTAN.getIso2LetterCode(), false);

    String query = builder.queryFilter(p);
    assertEquals("COUNTRY=AF", query);

    List<Predicate> ors = new ArrayList<>();
    ors.add(new EqualsPredicate(OccurrenceSearchParameter.YEAR, "2000", false));
    ors.add(new EqualsPredicate(OccurrenceSearchParameter.YEAR, "2001", false));
    ors.add(new EqualsPredicate(OccurrenceSearchParameter.YEAR, "2002", false));
    DisjunctionPredicate or = new DisjunctionPredicate(ors);

    query = builder.queryFilter(or);
    assertEquals("YEAR=2000&YEAR=2001&YEAR=2002", query);
  }

  @Test
  public void testNot() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();
    NotPredicate noBirds =
        new NotPredicate(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "212", false));
    assertThrows(IllegalArgumentException.class, () -> builder.queryFilter(noBirds));
  }

  @Test
  public void testLess() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.queryFilter(new LessThanPredicate(OccurrenceSearchParameter.YEAR, "1900")));
  }

  @Test
  public void testGreater() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            builder.queryFilter(new GreaterThanPredicate(OccurrenceSearchParameter.YEAR, "1900")));
  }

  @Test
  public void testPolygon() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();
    final String wkt = "POLYGON((30 10,10 20,20 40,40 40,30 10))";
    String query = builder.queryFilter(new WithinPredicate(wkt));
    assertEquals("GEOMETRY=30+10%2C10+20%2C20+40%2C40+40%2C30+10", query);
  }

  @Test
  public void testRange() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();

    List<Predicate> rangeAnd = new ArrayList<>();
    rangeAnd.add(new GreaterThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "2000"));
    rangeAnd.add(new LessThanOrEqualsPredicate(OccurrenceSearchParameter.YEAR, "2011"));
    Predicate range = new ConjunctionPredicate(rangeAnd);

    String query = builder.queryFilter(range);
    assertEquals("YEAR=2000%2C2011", query);
  }

  @Test
  public void testIsNotNull() {
    QueryParameterFilterBuilder builder = new QueryParameterFilterBuilder();
    String query =
        builder.queryFilter(new IsNotNullPredicate(OccurrenceSearchParameter.MEDIA_TYPE));
    assertEquals("MEDIA_TYPE=*", query);
  }
}
