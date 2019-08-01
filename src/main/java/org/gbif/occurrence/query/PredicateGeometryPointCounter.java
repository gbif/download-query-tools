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

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.gbif.api.model.occurrence.predicate.*;

public class PredicateGeometryPointCounter extends PredicateVisitor<Integer> {

  public Integer count(Predicate p) {
    if (p == null) {
      return 0;
    }
    return visit(p);
  }

  protected Integer visit(ConjunctionPredicate and) {
    return and.getPredicates().stream().mapToInt(this::visit).max().orElse(0);
  }

  protected Integer visit(DisjunctionPredicate or) {
    return or.getPredicates().stream().mapToInt(this::visit).sum();
  }

  protected Integer visit(EqualsPredicate predicate) { return 0; }
  protected Integer visit(InPredicate predicate) { return 0; }
  protected Integer visit(GreaterThanOrEqualsPredicate predicate) { return 0; }
  protected Integer visit(GreaterThanPredicate predicate) { return 0; }
  protected Integer visit(LessThanOrEqualsPredicate predicate) { return 0; }
  protected Integer visit(LessThanPredicate predicate) { return 0; }
  protected Integer visit(LikePredicate predicate) { return 0; }
  protected Integer visit(IsNotNullPredicate predicate) { return 0; }

  protected Integer visit(WithinPredicate within) {
    try {
      Geometry geometry = new WKTReader().read(within.getGeometry());
      return geometry.getNumPoints();
    } catch (ParseException e) {
      // The geometry has already been validated.
      throw new IllegalArgumentException("Exception parsing WKT", e);
    }
  }

  protected Integer visit(NotPredicate not) {
    return visit(not.getPredicate());
  }
}
