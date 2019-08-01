/*
 * Copyright 2019 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.query;

import org.gbif.api.model.occurrence.predicate.*;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

public class PredicateTaxonCounter extends PredicateVisitor<Integer> {

  public Integer count(Predicate p) {
    if (p == null) {
      return 0;
    }
    return visit(p);
  }

  protected Integer isTaxonParameter(OccurrenceSearchParameter param) {
    switch (param) {
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
        return 1;

      default:
        return 0;
    }
  }

  protected Integer visit(ConjunctionPredicate and) {
    return and.getPredicates().stream().mapToInt(this::visit).sum();
  }

  protected Integer visit(DisjunctionPredicate or) {
    return or.getPredicates().stream().mapToInt(this::visit).sum();
  }

  protected Integer visit(EqualsPredicate predicate) {
    return isTaxonParameter(predicate.getKey());
  }

  protected Integer visit(InPredicate predicate) {
    return predicate.getValues().size() * isTaxonParameter(predicate.getKey());
  }

  protected Integer visit(GreaterThanOrEqualsPredicate predicate) { return 0; }
  protected Integer visit(GreaterThanPredicate predicate) { return 0; }
  protected Integer visit(LessThanOrEqualsPredicate predicate) { return 0; }
  protected Integer visit(LessThanPredicate predicate) { return 0; }
  protected Integer visit(LikePredicate predicate) { return 0; }
  protected Integer visit(IsNotNullPredicate predicate) { return 0; }
  protected Integer visit(WithinPredicate within) { return 0; }

  protected Integer visit(NotPredicate not) {
    return visit(not.getPredicate());
  }
}
