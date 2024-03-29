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
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.LessThanPredicate;
import org.gbif.api.model.predicate.LikePredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.WithinPredicate;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class builds a query parameter filter usable for search links from a
 * {@link org.gbif.api.model.predicate.Predicate} hierarchy.
 * This class is not thread safe, create a new instance for every use if concurrent calls to {#queryFilter()} is
 * expected.
 */
public class QueryParameterFilterBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(QueryParameterFilterBuilder.class);
  private static final String WILDCARD = "*";
  private static final Pattern POLYGON_PATTERN =
      Pattern.compile("POLYGON\\s*\\(\\s*\\((.+)\\)\\s*\\)", Pattern.CASE_INSENSITIVE);

  private Map<SearchParameter, ArrayList<String>> filter;

  private enum State {
    ROOT,
    AND,
    OR
  }

  private State state;
  private SearchParameter lastParam;

  public synchronized String queryFilter(Predicate p) {
    StringBuilder b = new StringBuilder();
    boolean first = true;

    // build filter map
    filter = new HashMap<>();
    state = State.ROOT;
    lastParam = null;
    visit(p);

    // transform filter map into query string
    for (Map.Entry<SearchParameter, ArrayList<String>> entry : filter.entrySet()) {
      for (String val : entry.getValue()) {
        if (first) {
          first = false;
        } else {
          b.append("&");
        }
        b.append(entry.getKey().name());
        b.append("=");
        b.append(encode(val));
      }
    }
    return b.toString();
  }

  /** Encodes a URL String using StandardCharsets.UTF_8. */
  private static String encode(String val) {
    try {
      return URLEncoder.encode(val, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void visit(ConjunctionPredicate and) throws IllegalStateException {
    // ranges are allowed underneath root - try first
    try {
      visitRange(and);
      return;
    } catch (IllegalArgumentException e) {
      // must be a root AND
    }

    if (state != State.ROOT) {
      throw new IllegalStateException("AND must be a root predicate or a valid range");
    }
    state = State.AND;

    for (Predicate p : and.getPredicates()) {
      lastParam = null;
      visit(p);
    }
    state = State.ROOT;
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
    addQueryParam(lower.getKey(), range(lower.getValue(), upper.getValue()));
  }

  private String range(String from, String to) {
    if (from == null || from.length() == 0) {
      from = WILDCARD;
    }
    if (to == null || to.length() == 0) {
      to = WILDCARD;
    }
    return from + "," + to;
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
    addQueryParam(predicate.getKey(), predicate.getValue());
  }

  private void visit(IsNotNullPredicate predicate) {
    addQueryParam(predicate.getParameter(), "*");
  }

  private void visit(LikePredicate predicate) {
    throw new IllegalArgumentException("LIKE operator not supported in web queries");
  }

  private void visit(GreaterThanPredicate predicate) {
    throw new IllegalArgumentException(
        "GREATER_THAN_OPERATOR operator not supported in web queries");
  }

  private void visit(GreaterThanOrEqualsPredicate p) {
    addQueryParam(p.getKey(), range(p.getValue(), null));
  }

  private void visit(LessThanPredicate predicate) {
    throw new IllegalArgumentException("LESS_THAN_OPERATOR operator not supported in web queries");
  }

  private void visit(LessThanOrEqualsPredicate p) {
    addQueryParam(p.getKey(), range(null, p.getValue()));
  }

  private void visit(WithinPredicate within) {
    addQueryParam(OccurrenceSearchParameter.GEOMETRY, extractPolygonValues(within.getGeometry()));
  }

  private String extractPolygonValues(String withinValue) {
    Matcher m = POLYGON_PATTERN.matcher(withinValue);
    if (m.find()) {
      return m.group(1);
    }
    throw new IllegalArgumentException("No valid polygon WKT: " + withinValue);
  }

  private void visit(InPredicate in) {
    for (Object val : in.getValues()) {
      addQueryParam(in.getKey(), (String) val);
    }
  }

  private void visit(NotPredicate not) throws IllegalStateException {
    throw new IllegalArgumentException("NOT operator not supported in web queries");
  }

  private void addQueryParam(SearchParameter param, String value) {
    // verify that last param if existed was the same
    if (lastParam != null && param != lastParam) {
      throw new IllegalArgumentException("Mix of search params not supported");
    }

    if (!filter.containsKey(param)) {
      filter.put(param, new ArrayList<>());
    }
    filter.get(param).add(value);
    lastParam = param;
  }

  private void visit(Predicate p) throws IllegalStateException {
    Method method = null;
    try {
      method = getClass().getDeclaredMethod("visit", new Class[] {p.getClass()});
    } catch (NoSuchMethodException e) {
      LOG.warn(
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
          "This should never happen as we set accessible to true explicitly before. Probably a programming error",
          e);
      throw new RuntimeException("Programming error", e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the Hive Download", e);
      throw new IllegalArgumentException(e);
    }
  }
}
