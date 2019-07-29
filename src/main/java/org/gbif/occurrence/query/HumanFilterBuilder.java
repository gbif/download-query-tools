/*
 * Copyright 2014 Global Biodiversity Information Facility (GBIF)
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

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.MediaType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Map;
import java.util.ResourceBundle;

import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class builds a human readable filter from a {@link org.gbif.api.model.occurrence.predicate.Predicate} hierarchy.
 * This class is not thread safe, create a new instance for every use if concurrent calls to {#humanFilter} is expected.
 * This builder only supports predicates that follow our search query parameters style with multiple values for the same
 * parameter being logically disjunct (OR) while different search parameters are logically combined (AND). Therefore
 * the {#humanFilter(Predicate p)} result is a map of OccurrenceSearchParameter (AND'ed) to a list of values (OR'ed).
 */
@Deprecated
public class HumanFilterBuilder {

  private enum State {
    ROOT, AND, OR
  }

  private static final Logger LOG = LoggerFactory.getLogger(HumanFilterBuilder.class);
  private static final String DEFAULT_BUNDLE = "org/gbif/occurrence/query/filter";

  private static final String EQUALS_OPERATOR = "";
  private static final String GREATER_THAN_OPERATOR = "> ";
  private static final String GREATER_THAN_EQUALS_OPERATOR = "≥ ";
  private static final String LESS_THAN_OPERATOR = "< ";
  private static final String LESS_THAN_EQUALS_OPERATOR = "≤ ";

  private static final String NOT_OPERATOR = "not";
  private static final String IS_NOT_NULL_OPERATOR = "is not null";

  private static final String LIKE_OPERATOR = "~";
  private Map<OccurrenceSearchParameter, LinkedList<String>> filter;
  private State state;
  private OccurrenceSearchParameter lastParam;
  private final FilterLookupCounter filterLookupCounter = new FilterLookupCounter();
  private final TitleLookup titleLookup;
  private final ResourceBundle resourceBundle;

  public HumanFilterBuilder(TitleLookup titleLookup) {
    this.titleLookup = titleLookup;
    resourceBundle = ResourceBundle.getBundle(DEFAULT_BUNDLE);
  }

  /**
   * Creates a new human filter builder using the default resource bundle for enumerations from the GBIF API.
   */
  public HumanFilterBuilder(WebResource apiRoot) {
    resourceBundle = ResourceBundle.getBundle(DEFAULT_BUNDLE);
    titleLookup = new TitleLookup(apiRoot);
  }

  /**
   * @param p the predicate to convert
   * @return a list of anded parameters with multiple values to be combined with OR
   * @throws IllegalStateException if more complex predicates than the portal handles are supplied
   */
  public synchronized Map<OccurrenceSearchParameter, LinkedList<String>> humanFilter(Predicate p) {
    int count = filterLookupCounter.countLookups(p);
    if (count > 10050) {
      throw new IllegalStateException("Too many lookups ("+count+") would be needed.");
    }

    filter = Maps.newLinkedHashMap();
    state = State.ROOT;
    lastParam = null;
    if (p != null) {
      visit(p);
    }
    return filter;
  }

  /**
   * @param p the predicate to convert
   * @return a list of anded parameters with multiple values to be combined with OR
   * @throws IllegalStateException if more complex predicates than the portal handles are supplied
   */
  public synchronized String humanFilterString(Predicate p) {
    if (p == null) {
      return "All data";
    }
    humanFilter(p);
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<OccurrenceSearchParameter, LinkedList<String>> entry : filter.entrySet()) {
      if (sb.length() > 0) {
        sb.append(" \n");
      }
      sb.append(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, entry.getKey().name()));
      sb.append(": ");
      boolean delimit = false;
      for (String val : entry.getValue()) {
        if (delimit) {
          sb.append(" or ");
        } else {
          delimit = true;
        }
        sb.append(val);
      }
    }
    return sb.toString();
  }

  private void addParamValue(OccurrenceSearchParameter param, String op, String value) {
    addParamValue(param, op + getHumanValue(param, value));
  }

  /**
   * Gets the human readable value of the parameter value.
   */
  private String getHumanValue(OccurrenceSearchParameter param, String value) {
    String humanValue;
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
        humanValue = titleLookup.getSpeciesName(value);
        break;
      case DATASET_KEY:
        humanValue = titleLookup.getDatasetTitle(value);
        break;
      case COUNTRY:
      case PUBLISHING_COUNTRY:
        humanValue = lookupCountryCode(value);
        break;
      case CONTINENT:
        humanValue = lookupContinent(value);
        break;
      case MONTH:
        humanValue = lookupMonth(value);
        break;

      default:
        if (param.type().isEnum()) {
          humanValue = lookupEnum(param.type(), value);
        } else {
          humanValue = value;
        }
    }
    // add unit symbol
    if (param==OccurrenceSearchParameter.DEPTH || param==OccurrenceSearchParameter.ELEVATION) {
      humanValue = humanValue + "m";
    }
    return humanValue;
  }

  private String lookupContinent(String value) {
    Continent c = VocabularyUtils.lookupEnum(value, Continent.class);
    return c.getTitle();
  }

  private void addParamValue(OccurrenceSearchParameter param, String op) {
    // verify that last param if existed was the same
    /*if (lastParam != null && param != lastParam) {
      throw new IllegalArgumentException("Mix of search params not supported: "+param);
    }  */

    if (!filter.containsKey(param)) {
      filter.put(param, Lists.<String>newLinkedList());
    }

    filter.get(param).add(op);
    lastParam = param;
  }

  private String lookupEnum(Class clazz, String value) {
    return resourceBundle.getString("enum." + clazz.getSimpleName().toLowerCase() + "." + (clazz == MediaType.class? value.trim() : value.trim().toUpperCase()));
  }

  private String lookupCountryCode(String code) {
    Country c = Country.fromIsoCode(code);
    if (c != null) {
      return c.getTitle();
    }
    return code;
  }

  private String lookupMonth(String month) {
    //It's a range
    String[] monthRange = month.split("-");
    if(monthRange.length == 2) {
      return resourceBundle.getString("enum.month." + monthRange[0]) + "-"  + resourceBundle.getString("enum.month." + monthRange[1]);
    }
    return resourceBundle.getString("enum.month." + month);
  }

  private void visit(ConjunctionPredicate and) throws IllegalStateException {
    // ranges are allowed underneath root - try first
    try {
      visitRange(and);
      return;
    } catch (IllegalArgumentException e) {
      // must be a root AND
    }

   /* if (state != State.ROOT) {
      throw new IllegalStateException("AND must be a root predicate or a valid range");
    }     */
    state = State.AND;

    for (Predicate p : and.getPredicates()) {
      lastParam = null;
      visit(p);
    }
    //state = State.ROOT;
  }

  private void visit(DisjunctionPredicate or) throws IllegalStateException {
    State oldState = state;
    if (state == State.OR) {
      throw new IllegalStateException("OR within OR filters not supported");
    }
    state = State.OR;

    for (Predicate p : or.getPredicates()) {
      visit(p);
    }
    state = oldState;
  }

  private void visit(EqualsPredicate predicate) {
    addParamValue(predicate.getKey(), EQUALS_OPERATOR, predicate.getValue());
  }

  private void visit(GreaterThanOrEqualsPredicate predicate) {
    addParamValue(predicate.getKey(), GREATER_THAN_EQUALS_OPERATOR, predicate.getValue());
  }

  private void visit(GreaterThanPredicate predicate) {
    addParamValue(predicate.getKey(), GREATER_THAN_OPERATOR, predicate.getValue());
  }

  private void visit(InPredicate in) {
    for (String val : in.getValues()) {
      addParamValue(in.getKey(), EQUALS_OPERATOR, val);
    }
  }

  private void visit(LessThanOrEqualsPredicate predicate) {
    addParamValue(predicate.getKey(), LESS_THAN_EQUALS_OPERATOR, predicate.getValue());
  }

  private void visit(LessThanPredicate predicate) {
    addParamValue(predicate.getKey(), LESS_THAN_OPERATOR, predicate.getValue());
  }

  private void visit(LikePredicate predicate) {
    addParamValue(predicate.getKey(), LIKE_OPERATOR, predicate.getValue());
  }

  private void visit(NotPredicate not) throws IllegalStateException {
    if (not.getPredicate() instanceof SimplePredicate) {
      visit(not.getPredicate());
      SimplePredicate sp = (SimplePredicate) not.getPredicate();
      // now prefix the last value with NOT
      String notValue = NOT_OPERATOR + " (" + filter.get(sp.getKey()).removeLast() + ")";
      filter.get(sp.getKey()).add(notValue);

    } else {
      throw new IllegalArgumentException("NOT predicate must be followed by a simple predicate");
    }
  }

  private void visit(IsNotNullPredicate predicate) {
    addParamValue(predicate.getParameter(), IS_NOT_NULL_OPERATOR);
  }


  private void visit(Predicate p) throws IllegalStateException {
    Method method = null;
    try {
      method = getClass().getDeclaredMethod("visit", new Class[] {p.getClass()});
    } catch (NoSuchMethodException e) {
      LOG
        .warn(
          "Visit method could not be found. That means a Predicate has been passed in that is unknown to this "
            + "class",
          e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.setAccessible(true);
      method.invoke(this, p);
    } catch (IllegalAccessException e) {
      LOG.error(
        "This should never happen as we set accessible to true explicitly before. Probably a programming error", e);
      throw new RuntimeException("Programming error", e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the human query string", e);
      throw new IllegalArgumentException(e);
    }
  }

  private void visit(WithinPredicate within) {
    addParamValue(OccurrenceSearchParameter.GEOMETRY, "", within.getGeometry());
  }

  private void visitRange(ConjunctionPredicate and) {
    if (and.getPredicates().size() != 2) {
      throw new IllegalArgumentException("no valid range");
    }
    GreaterThanOrEqualsPredicate lower = null;
    LessThanOrEqualsPredicate upper = null;
    for (Predicate p : and.getPredicates()) {
      if (p instanceof GreaterThanOrEqualsPredicate) {
        lower = (GreaterThanOrEqualsPredicate) p;
      } else if (p instanceof LessThanOrEqualsPredicate) {
        upper = (LessThanOrEqualsPredicate) p;
      }
    }
    if (lower == null || upper == null || lower.getKey() != upper.getKey()) {
      throw new IllegalArgumentException("no valid range");
    }
    addParamValue(lower.getKey(), "", lower.getValue() + "-" + upper.getValue());
  }

}
