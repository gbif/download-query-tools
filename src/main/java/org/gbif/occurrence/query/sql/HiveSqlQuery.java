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

import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.gbif.api.exception.QueryBuildingException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;

import lombok.Getter;

/**
 * A parsed, validated Hive SQL query.
 *
 * Exposes useful parts of the query.
 */
@Getter
public class HiveSqlQuery {

  /*
   * SQL string for internal use — validation, execution using Hive.
   */
  final String sql;

  /*
   * SQL WHERE clause string for internal use — validation, execution using Hive.
   */
  final String sqlWhere;

  /*
   * User-facing SQL string — nicely formatted, and without internal catalogue/table names or other
   * implementation concerns.
   */
  final String userSql;

  final List<String> sqlSelectColumnNames;
  final Integer predicateCount;
  final Integer pointsCount;

  /**
   * Parse and validate the query.  Throws an exception if parsing/validation fails.
   */
  public HiveSqlQuery(HiveSqlValidator sqlValidator, String unvalidatedSql)
      throws QueryBuildingException {
    this(sqlValidator, unvalidatedSql, null);
  }

  /**
   * Parse and validate the query.  Throws an exception if parsing/validation fails.
   */
  public HiveSqlQuery(HiveSqlValidator sqlValidator, String unvalidatedSql, String catalog)
      throws QueryBuildingException {
    SqlDialect sqlDialect = sqlValidator.getDialect();

    SqlSelect node = sqlValidator.validate(unvalidatedSql, catalog);

    // Nicely formatted SQL
    UnaryOperator<SqlWriterConfig> sqlWriterConfig = c ->
      c.withDialect(Util.first(sqlDialect, AnsiSqlDialect.DEFAULT))
        .withClauseStartsLine(true)
        .withClauseEndsLine(true)
        .withIndentation(2)
        .withAlwaysUseParentheses(false)
        .withQuoteAllIdentifiers(true)
        .withLineFolding(SqlWriterConfig.LineFolding.TALL);

    // Internal SQL
    this.sql = node.toSqlString(sqlDialect).getSql();

    // Nicely formatted SQL for the user
    this.userSql = node.toSqlString(sqlWriterConfig).getSql();

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
    Map<SqlKind, Integer> count = node.accept(new KindValidatorAndCounterVisitor());
    predicateCount =
        count.getOrDefault(SqlKind.LITERAL, 0)
            + count.getOrDefault(SqlKind.AND, 0)
            + count.getOrDefault(SqlKind.OR, 0);

    // Count points in geometry within queries
    pointsCount = node.accept(new GeometryPointCounterVisitor());
  }
}
