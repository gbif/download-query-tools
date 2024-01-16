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
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class counts the number of webservice lookups needed to format a {@link Predicate} hierarchy.
 */
public class PredicateLookupCounter extends PredicateVisitor<Integer> {

  protected static final Logger LOG = LoggerFactory.getLogger(PredicateLookupCounter.class);

  public Integer count(Predicate p) {
    if (p == null) {
      return 0;
    }
    return visit(p);
  }

  protected Integer getHumanValue(SearchParameter param) {
    // lookup values
    if (param instanceof OccurrenceSearchParameter) {
      switch ((OccurrenceSearchParameter) param) {
        case SCIENTIFIC_NAME:
        case ACCEPTED_TAXON_KEY:
        case TAXON_KEY:
        case KINGDOM_KEY:
        case PHYLUM_KEY:
        case CLASS_KEY:
        case ORDER_KEY:
        case FAMILY_KEY:
        case GENUS_KEY:
        case SUBGENUS_KEY:
        case SPECIES_KEY:
        case DATASET_KEY:
          return 1;
        default:
          return 0;
      }
    } else {
      return 0;
    }
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
