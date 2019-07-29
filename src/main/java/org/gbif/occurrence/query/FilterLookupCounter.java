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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This class counts the number of webservice lookups needed to format a {@link Predicate} hierarchy.
 */
class FilterLookupCounter {

  private static final Logger LOG = LoggerFactory.getLogger(FilterLookupCounter.class);

  /**
   * @param p the predicate to convert
   * @return the number of lookups necessary
   * @throws IllegalStateException if more complex predicates than the portal handles are supplied
   */
  public synchronized int countLookups(Predicate p) {
    if (p == null) {
      return 0;
    }
    return visit(p);
  }

  /**
   * Gets the human readable value of the parameter value.
   */
  private int getHumanValue(OccurrenceSearchParameter param) {
    // lookup values
    switch (param) {
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
  }

  private int visit(ConjunctionPredicate and) throws IllegalStateException {
    int count = 0;
    for (Predicate p : and.getPredicates()) {
      count += visit(p);
    }
    return count;
  }

  private int visit(DisjunctionPredicate or) throws IllegalStateException {
    int count = 0;
    for (Predicate p : or.getPredicates()) {
      count += visit(p);
    }
    return count;
  }

  private int visit(EqualsPredicate predicate) {
    return getHumanValue(predicate.getKey());
  }

  private int visit(InPredicate in) {
    return in.getValues().size() * getHumanValue(in.getKey());
  }

  private int visit(GreaterThanOrEqualsPredicate predicate) { return 0; }

  private int visit(GreaterThanPredicate predicate) { return 0; }

  private int visit(LessThanOrEqualsPredicate predicate) { return 0; }

  private int visit(LessThanPredicate predicate) { return 0; }

  private int visit(LikePredicate predicate) { return 0; }

  private int visit(IsNotNullPredicate predicate) { return 0; }

  private int visit(WithinPredicate within) { return 0; }

  private int visitRange(ConjunctionPredicate and) { return 0; }

  private int visit(NotPredicate not) throws IllegalStateException {
    return visit(not.getPredicate());
  }

  private int visit(Predicate p) throws IllegalStateException {
    Method method = null;
    try {
      method = getClass().getDeclaredMethod("visit", new Class[] {p.getClass()});
    } catch (NoSuchMethodException e) {
      LOG.warn("Visit method could not be found. That means a Predicate has been passed in that is unknown to this class", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.setAccessible(true);
      return (Integer) method.invoke(this, p);
    } catch (IllegalAccessException e) {
      LOG.error("This should never happen as we set accessible to true explicitly before. Probably a programming error", e);
      throw new RuntimeException("Programming error", e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the human query string", e);
      throw new IllegalArgumentException(e);
    }
  }
}
