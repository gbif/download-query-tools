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

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.*;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.api.model.occurrence.search.OccurrenceSearchParameter.*;

/**
 * This class counts the number of webservice lookups needed to format a {@link Predicate} hierarchy.
 */
@Slf4j
public class PredicateLookupCounter extends PredicateVisitor<Integer> {

  public Integer count(Predicate p) {
    if (p == null) {
      return 0;
    }
    return visit(p);
  }

  protected Integer getHumanValue(SearchParameter param) {
    // lookup values
    if (List.of(
            SCIENTIFIC_NAME,
            ACCEPTED_TAXON_KEY,
            TAXON_KEY,
            KINGDOM_KEY,
            PHYLUM_KEY,
            CLASS_KEY,
            ORDER_KEY,
            FAMILY_KEY,
            GENUS_KEY,
            SUBGENUS_KEY,
            SPECIES_KEY,
            DATASET_KEY)
        .contains(param)) {
      return 1;
    }
    return 0;
  }

  @Override
  protected Integer visit(ConjunctionPredicate and) {
    int count = 0;
    for (Predicate p : and.getPredicates()) {
      count += visit(p);
    }
    return count;
  }

  @Override
  protected Integer visit(DisjunctionPredicate or) {
    int count = 0;
    for (Predicate p : or.getPredicates()) {
      count += visit(p);
    }
    return count;
  }

  @Override
  protected Integer visit(EqualsPredicate predicate) {
    return getHumanValue(predicate.getKey());
  }

  @Override
  protected Integer visit(InPredicate predicate) {
    return predicate.getValues().size() * getHumanValue(predicate.getKey());
  }

  @Override
  protected Integer visit(GreaterThanOrEqualsPredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(GreaterThanPredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(LessThanOrEqualsPredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(LessThanPredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(LikePredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(IsNullPredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(IsNotNullPredicate predicate) {
    return 0;
  }

  @Override
  protected Integer visit(WithinPredicate within) {
    return 0;
  }

  protected Integer visitRange(ConjunctionPredicate and) {
    return 0;
  }

  @Override
  protected Integer visit(NotPredicate not) {
    return visit(not.getPredicate());
  }

  @Override
  protected Integer visit(GeoDistancePredicate predicate) {
    return 0;
  }
}
