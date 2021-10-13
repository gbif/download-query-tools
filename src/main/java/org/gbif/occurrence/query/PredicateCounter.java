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

public class PredicateCounter extends PredicateVisitor<Integer> {

  public Integer count(Predicate p) {
    if (p == null) {
      return 0;
    }
    return visit(p);
  }

  @Override
  protected Integer visit(ConjunctionPredicate and) {
    return and.getPredicates().stream().mapToInt(this::visit).sum();
  }

  @Override
  protected Integer visit(DisjunctionPredicate or) {
    return or.getPredicates().stream().mapToInt(this::visit).sum();
  }

  @Override
  protected Integer visit(EqualsPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(InPredicate predicate) {
    return predicate.getValues().size();
  }

  @Override
  protected Integer visit(GreaterThanOrEqualsPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(GreaterThanPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(LessThanOrEqualsPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(LessThanPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(LikePredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(IsNullPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(IsNotNullPredicate predicate) {
    return 1;
  }

  @Override
  protected Integer visit(WithinPredicate within) {
    return 1;
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
