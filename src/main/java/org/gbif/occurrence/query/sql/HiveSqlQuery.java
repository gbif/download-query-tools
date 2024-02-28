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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;

/**
 * A parsed, validated Hive SQL query.
 *
 * Exposes useful parts of the query.
 */
public class HiveSqlQuery {

  final String sql;
  final String sqlWhere;
  final List<String> sqlSelectColumnNames;
  final Integer predicateCount;
  final Integer pointsCount;

  /**
   * Parse and validate the query.  Throws an exception if parsing/validation fails.
   */
  public HiveSqlQuery(HiveSqlValidator sqlValidator, String unvalidatedSql) {
    SqlDialect sqlDialect = sqlValidator.getDialect();

    SqlSelect node = sqlValidator.validate(unvalidatedSql);

    this.sql = node.toSqlString(sqlDialect).getSql();
    if (node.getWhere() != null) {
      this.sqlWhere = node.getWhere().toSqlString(sqlDialect).getSql();
    } else {
      this.sqlWhere = "1 = 1";
    }

    sqlSelectColumnNames = new ArrayList<>();

    // Finds suitable column names from the SQL select part.  Rather than the typical database
    // naming of "c0" etc
    // for expressions, we return the expression. They might need further cleaning!
    for (SqlNode n : node.getSelectList().getList()) {
      if (SqlKind.IDENTIFIER == n.getKind()) {
        SqlIdentifier i = (SqlIdentifier) n;
        sqlSelectColumnNames.add(Util.last(i.names));
      } else if (SqlKind.AS == n.getKind()) {
        SqlBasicCall a = (SqlBasicCall) n;
        sqlSelectColumnNames.add(Util.last(a.getOperandList()).toSqlString(sqlDialect).getSql());
      } else {
        sqlSelectColumnNames.add(n.toSqlString(sqlDialect).getSql());
      }
    }

    // Count predicates
    Map<SqlKind, Integer> count = node.accept(new KindCounterVisitor());
    predicateCount =
        count.getOrDefault(SqlKind.LITERAL, 0)
            + count.getOrDefault(SqlKind.AND, 0)
            + count.getOrDefault(SqlKind.OR, 0);

    // Count points in geometry within queries
    pointsCount = node.accept(new GeometryPointCounterVisitor());
  }

  public String getSql() {
    return sql;
  }

  public String getSqlWhere() {
    return sqlWhere;
  }

  public List<String> getSqlSelectColumnNames() {
    return sqlSelectColumnNames;
  }

  public Integer getPredicateCount() {
    return predicateCount;
  }

  public Integer getPointsCount() {
    return pointsCount;
  }
}
