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
package org.gbif.occurrence.query.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;

import com.google.common.collect.ImmutableList;

/**
 * Validates and counts parts of SQL queries
 */
class KindValidatorAndCounterVisitor implements SqlVisitor<Map<SqlKind, Integer>> {

  private ImmutableList<String> mistakenAsFields = ImmutableList.of("year", "month", "day");

  Map<SqlKind, Integer> incMap(Map<SqlKind, Integer> m, SqlKind k) {
    if (m == null) {
      m = new HashMap<>();
    }
    m.put(k, m.getOrDefault(k, 0) + 1);
    return m;
  }

  Map<SqlKind, Integer> addMaps(Map<SqlKind, Integer> m1, Map<SqlKind, Integer> m2) {
    Map<SqlKind, Integer> result =
        Stream.concat(m1.entrySet().stream(), m2.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));
    return result;
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlCall call) {
    Map<SqlKind, Integer> m = new HashMap<>();
    for (SqlNode n : call.getOperandList()) {
      if (n != null) {
        switch (n.getKind()) {
          case CAST:
            // Check this isn't something like "CAST('year' AS INTEGER)" which happens when the user
            // does something like
            // "'year' > 2000" and Calcite tries to fix it.
            //
            // This is handled specially because it's a common error for someone new to the SQL API.
            SqlBasicCall castCall = (SqlBasicCall) n;
            if (castCall.operandCount() == 2) {
              SqlNode first = castCall.getOperandList().get(0);
              SqlNode second = castCall.getOperandList().get(1);
              if (first instanceof SqlLiteral
                  && mistakenAsFields.contains(((SqlLiteral) first).toValue())) {
                if (second instanceof SqlDataTypeSpec
                    && ((SqlDataTypeSpec) second).getTypeName().names.get(0).equals("INTEGER")) {
                  throw new SqlValidationException(
                      "'year', 'month' or 'day' string literals used in a comparison. (Hint: use double quotes for \"year\", \"month\" and \"day\" columns.)");
                }
              }
            }
            break;

          case IS_TRUE:
          case IS_NOT_TRUE:
          case IS_FALSE:
          case IS_NOT_FALSE:
            // These are not supported by our old Hive version.  Remove the restriction with Hive 3
            // or later.
            throw new SqlValidationException(
                "x IS TRUE and x IS FALSE are not supported, please use x = TRUE and x = FALSE instead.");

          case BETWEEN:
            // This gets changed into "BETWEEN ASYMMETRIC" by Calcite which Hive doesn't support.
            // See https://issues.apache.org/jira/browse/CALCITE-4471 in case Calcite fix this.
            throw new SqlValidationException(
              "BETWEEN is not supported, please use comparison operators (<, <=, >=, >) instead.");
        }
        m = addMaps(m, n.accept(this));
      }
    }
    return incMap(m, call.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlNodeList nodeList) {
    Map<SqlKind, Integer> m = new HashMap<>();
    for (SqlNode n : nodeList.getList()) {
      if (n != null) m = addMaps(m, n.accept(this));
    }
    return incMap(m, nodeList.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlLiteral literal) {
    return incMap(null, literal.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlIdentifier id) {
    return incMap(null, id.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlDataTypeSpec type) {
    return incMap(null, type.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlDynamicParam param) {
    return incMap(null, param.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visit(SqlIntervalQualifier intervalQualifier) {
    return incMap(null, intervalQualifier.getKind());
  }

  @Override
  public Map<SqlKind, Integer> visitNode(SqlNode n) {
    return n.accept(this);
  }

  class SqlValidationException extends RuntimeException {
    SqlValidationException(String message) {
      super(message);
    }
  }
}
