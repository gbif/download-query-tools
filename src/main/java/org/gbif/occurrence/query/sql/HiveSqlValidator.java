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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HiveSqlValidator {
  private static Logger LOG = LoggerFactory.getLogger(HiveSqlValidator.class);

  private final SqlDialect dialect;
  private final SqlParser.Config parserConfig;
  private final SqlValidator.Config validatorConfig;
  private final SchemaPlus rootSchema;
  private final FrameworkConfig frameworkConfig;
  private final RelDataTypeFactory relDataTypeFactory;
  private final CalciteCatalogReader catalogReader;
  private final SqlOperatorTable sqlOperatorTable;
  private final SqlValidator validator;

  public HiveSqlValidator(SchemaPlus rootSchema) {
    dialect = new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT.withDatabaseMajorVersion(3));

    parserConfig = SqlParser.Config.DEFAULT
      .withParserFactory(SqlParserImpl.FACTORY)
      .withQuoting(Quoting.DOUBLE_QUOTE)
      .withUnquotedCasing(Casing.TO_LOWER)
      .withConformance(SqlConformanceEnum.LENIENT);

    validatorConfig = SqlValidatorImpl.Config.DEFAULT
      .withConformance(SqlConformanceEnum.LENIENT)
      .withColumnReferenceExpansion(false);

    // Custom functions
    SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
    SqlFunction stringArrayContainsFunction = new SqlFunction("stringArrayContains",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.BOOLEAN),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);
    sqlStdOperatorTable.register(stringArrayContainsFunction);

    SqlFunction eeaCellCodeFunction = new SqlFunction("eeaCellCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION);
    sqlStdOperatorTable.register(eeaCellCodeFunction);

    this.rootSchema = rootSchema;
    this.frameworkConfig =  Frameworks.newConfigBuilder()
      .parserConfig(parserConfig)
      .sqlValidatorConfig(validatorConfig)
      .defaultSchema(rootSchema)
      .operatorTable(sqlStdOperatorTable)
      .build();
    this.relDataTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
    this.catalogReader = new CalciteCatalogReader(
      CalciteSchema.from(rootSchema),
      CalciteSchema.from(rootSchema).path(rootSchema.getName()),
      relDataTypeFactory,
      new CalciteConnectionConfigImpl(properties));
    this.sqlOperatorTable = SqlOperatorTables.chain(frameworkConfig.getOperatorTable(), catalogReader);
    this.validator = SqlValidatorUtil.newValidator(sqlOperatorTable, catalogReader, relDataTypeFactory, frameworkConfig.getSqlValidatorConfig());
  }

  public SqlSelect validate(String sql) {
    LOG.debug("Parsing {}", sql);
    SqlParser sqlParser = SqlParser.create(sql, frameworkConfig.getParserConfig());
    try {
      SqlNode sqlNode = sqlParser.parseQuery();
      SqlNode validatedSqlNode = validator.validate(sqlNode);
      LOG.debug("Validated as {}", validatedSqlNode.toSqlString(dialect));

      if (validatedSqlNode.getKind() != SqlKind.SELECT) {
        LOG.warn("Rejected as only SELECT statements are supported; {} → {}.", sql, validatedSqlNode.getKind());
        throw new RuntimeException("Only SQL SELECT statements are supported.");
      }

      SqlSelect select = (SqlSelect) validatedSqlNode;
      LOG.trace("- Operator: " + select.getOperator());

      if (select.hasHints()) {
        LOG.warn("Rejected as hints supported; {} → {}.", sql, select.getHints());
        throw new RuntimeException("SQL hints are not supported.");
      }

      if (select.getModifierNode(SqlSelectKeyword.STREAM) != null) {
        LOG.warn("Rejected as streams are not supported; {} → {}.", sql, select.getModifierNode(SqlSelectKeyword.STREAM));
        throw new RuntimeException("SQL streams are not supported.");
      }

      LOG.trace("- ModNod-ALL: " + select.getModifierNode(SqlSelectKeyword.ALL));
      LOG.trace("- ModNod-DISTINCT: " + select.getModifierNode(SqlSelectKeyword.DISTINCT));

      LOG.trace("- SelectList: " + select.getSelectList());
      LOG.trace("- From: " + select.getFrom());
      LOG.trace("- Where: " + select.getWhere());
      LOG.trace("- Group: " + select.getGroup());

      if (select.getHaving() != null) {
        LOG.warn("Rejected as having clauses are not supported; {} → {}.", sql, select.getHaving());
        throw new RuntimeException("SQL having clauses are not supported.");
      }

      if (!select.getWindowList().isEmpty() || select.getQualify() != null) {
        LOG.warn("Rejected as window functions are not supported; {} → {} / {}.", sql, select.getWindowList(), select.getQualify());
        throw new RuntimeException("SQL window functions are not supported.");
      }

      LOG.trace("- OrderList: " + select.getOrderList());
      LOG.trace("- Fetch: " + select.getFetch());
      LOG.trace("- Offset: " + select.getOffset());
      //LOG.trace("- OpList: " + select.getOperandList());

      for (SqlNode n : select.getSelectList().getList()) {
        if (n instanceof SqlIdentifier) {
          SqlIdentifier id = (SqlIdentifier) n;
          if (id.isStar()) {
            LOG.warn("Rejected as star selects are not supported; {} → {}.", sql, id);
            throw new RuntimeException("Star selects are not supported.");
          }
        }
      }

      Map<SqlKind,Integer> count = select.accept(new KindCounterVisitor());
      LOG.debug("Count: " + count);
      if (count.getOrDefault(SqlKind.SELECT, -1) != 1) {
        LOG.warn("Rejected as multiple selects present; {} → {}.", sql);
        throw new RuntimeException("Must be exactly one SQL select statement.");
      }

      if (count.getOrDefault(SqlKind.JOIN, 0) > 0) {
        LOG.warn("Rejected as joins present; {} → {}.", sql);
        throw new RuntimeException("Joins are not supported.");
      }

      return select;
    } catch (Exception e) {
      LOG.warn("Rejected for another reason; {} → {}.", sql, e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /**
   * Counts parts of SQL queries
   */
  class KindCounterVisitor implements SqlVisitor<Map<SqlKind,Integer>> {

    Map<SqlKind, Integer> incMap(Map<SqlKind,Integer> m, SqlKind k) {
      if (m == null) {
        m = new HashMap<>();
      }
      m.put(k, m.getOrDefault(k, 0) + 1);
      return m;
    }

    Map<SqlKind, Integer> addMaps(Map<SqlKind,Integer> m1, Map<SqlKind,Integer> m2) {
      Map<SqlKind, Integer> result = Stream.concat(m1.entrySet().stream(), m2.entrySet().stream())
        .collect(Collectors.toMap(
          Map.Entry::getKey,
          Map.Entry::getValue,
          Integer::sum));
      return result;
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlCall call) {
      Map<SqlKind, Integer> m = new HashMap<>();
      for (SqlNode n : call.getOperandList()) {
        if (n != null) m = addMaps(m, n.accept(this));
      }
      return incMap(m, call.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlNodeList nodeList) {
      Map<SqlKind, Integer> m = new HashMap<>();
      for (SqlNode n : nodeList.getList()) {
        if (n != null) m = addMaps(m, n.accept(this));
      }
      return incMap(m, nodeList.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlLiteral literal) {
      return incMap(null, literal.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlIdentifier id) {
      return incMap(null, id.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlDataTypeSpec type) {
      return incMap(null, type.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlDynamicParam param) {
      return incMap(null, param.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visit(SqlIntervalQualifier intervalQualifier) {
      return incMap(null, intervalQualifier.getKind());
    }

    @Override
    public Map<SqlKind,Integer> visitNode(SqlNode n) {
      return n.accept(this);
    }
  }

  public SqlDialect getDialect() {
    return dialect;
  }
}
