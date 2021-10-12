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

import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.IsNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PredicateVisitor<T> {

  private static final Logger LOG = LoggerFactory.getLogger(PredicateVisitor.class);

  protected abstract T visit(ConjunctionPredicate and);

  protected abstract T visit(DisjunctionPredicate or);

  protected abstract T visit(EqualsPredicate predicate);

  protected abstract T visit(InPredicate predicate);

  protected abstract T visit(GreaterThanOrEqualsPredicate predicate);

  protected abstract T visit(GreaterThanPredicate predicate);

  protected abstract T visit(LessThanOrEqualsPredicate predicate);

  protected abstract T visit(LessThanPredicate predicate);

  protected abstract T visit(LikePredicate predicate);

  protected abstract T visit(IsNotNullPredicate predicate);

  protected abstract T visit(IsNullPredicate predicate);

  protected abstract T visit(WithinPredicate within);

  protected abstract T visit(NotPredicate not);

  public T visit(Predicate p) throws IllegalStateException {
    Method method = null;
    try {
      method = getClass().getDeclaredMethod("visit", new Class[] {p.getClass()});
    } catch (NoSuchMethodException e) {
      LOG.warn(
          "Visit method could not be found. That means a Predicate has been passed in that is unknown to this class",
          e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.setAccessible(true);
      return (T) method.invoke(this, p);
    } catch (IllegalAccessException e) {
      LOG.error(
          "This should never happen as we set accessible to true explicitly before. Probably a programming error",
          e);
      throw new RuntimeException("Programming error", e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while visiting predicates", e);
      throw new IllegalArgumentException(e);
    }
  }
}
