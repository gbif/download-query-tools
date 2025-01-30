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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Util;
import org.gbif.api.exception.QueryBuildingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveSqlValidator {
  private static Logger LOG = LoggerFactory.getLogger(HiveSqlValidator.class);

  private static final Pattern SEMICOLON_END = Pattern.compile("[;\\s]*;\\s*$");

  private final SqlDialect dialect;
  private final SqlParser.Config parserConfig;
  private final SqlValidator.Config validatorConfig;
  private final SchemaPlus rootSchema;
  private final FrameworkConfig frameworkConfig;
  private final RelDataTypeFactory relDataTypeFactory;
  private final CalciteCatalogReader catalogReader;
  private final SqlOperatorTable sqlOperatorTable;
  private final SqlValidator validator;
  private final UnaryOperator<SqlWriterConfig> sqlDebugWriterConfig;

  public HiveSqlValidator(SchemaPlus rootSchema, List<SqlOperator> additionalOperators) {
    // dialect = SqlDialect.DatabaseProduct.HIVE.getDialect();
    dialect = new HiveSqlDialect(HiveSqlDialect.DEFAULT_CONTEXT.withDatabaseMajorVersion(3));

    sqlDebugWriterConfig = c ->
      c.withDialect(Util.first(dialect, AnsiSqlDialect.DEFAULT))
        .withClauseStartsLine(true)
        .withClauseEndsLine(true)
        .withIndentation(2)
        .withAlwaysUseParentheses(false)
        .withQuoteAllIdentifiers(true)
        .withLineFolding(SqlWriterConfig.LineFolding.TALL);

    parserConfig =
        SqlParser.Config.DEFAULT
            .withParserFactory(SqlParserImpl.FACTORY)
            .withQuoting(Quoting.DOUBLE_QUOTE)
            .withUnquotedCasing(Casing.TO_LOWER)
            .withConformance(SqlConformanceEnum.LENIENT);

    validatorConfig =
        SqlValidatorImpl.Config.DEFAULT
            .withConformance(SqlConformanceEnum.LENIENT)
            .withColumnReferenceExpansion(false)
            .withCallRewrite(false); // Disable rewriting COALESCE as CASE WHEN, etc.

    SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
    // Built-in Hive functions
    HiveSqlOperatorTable.instance().getAdditionalOperators().stream()
        .forEach(sqlStdOperatorTable::register);
    // Custom functions
    additionalOperators.stream().forEach(sqlStdOperatorTable::register);

    this.rootSchema = rootSchema;
    this.frameworkConfig =
        Frameworks.newConfigBuilder()
            .parserConfig(parserConfig)
            .sqlValidatorConfig(validatorConfig)
            .defaultSchema(rootSchema)
            .operatorTable(sqlStdOperatorTable)
            .build();
    this.relDataTypeFactory = new SqlTypeFactoryImpl(dialect.getTypeSystem());
    Properties properties = new Properties();
    properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "true");
    this.catalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(rootSchema).path(rootSchema.getName()),
            relDataTypeFactory,
            new CalciteConnectionConfigImpl(properties));
    this.sqlOperatorTable =
        SqlOperatorTables.chain(frameworkConfig.getOperatorTable(), catalogReader);
    this.validator =
        SqlValidatorUtil.newValidator(
            sqlOperatorTable,
            catalogReader,
            relDataTypeFactory,
            frameworkConfig.getSqlValidatorConfig());
  }

  public SqlSelect validate(String sql) throws QueryBuildingException {
    return validate(sql, null);
  }

  public SqlSelect validate(String sql, String catalog) throws QueryBuildingException {
    LOG.debug("Parsing «{}»", sql);
    Matcher m = SEMICOLON_END.matcher(sql);
    if (m.find()) {
      sql = m.replaceAll("");
      LOG.debug("Stripped trailing semicolon(s) «{}»", sql);
    }
    SqlParser sqlParser = SqlParser.create(sql, frameworkConfig.getParserConfig());
    try {
      SqlNode sqlNode = sqlParser.parseQuery();
      SqlNode validatedSqlNode = validator.validate(sqlNode);

      LOG.debug("Validated as {}", validatedSqlNode.toSqlString(sqlDebugWriterConfig));

      if (validatedSqlNode.getKind() != SqlKind.SELECT) {
        LOG.warn(
            "Rejected as only SELECT statements are supported; {} → {}.",
            sql,
            validatedSqlNode.getKind());
        throw new QueryBuildingException("Only SQL SELECT statements are supported.");
      }

      SqlSelect select = (SqlSelect) validatedSqlNode;
      LOG.trace("- Operator: {}", select.getOperator());

      if (select.hasHints()) {
        LOG.warn("Rejected as hints supported; {} → {}.", sql, select.getHints());
        throw new QueryBuildingException("SQL hints are not supported.");
      }

      if (select.getModifierNode(SqlSelectKeyword.STREAM) != null) {
        LOG.warn(
            "Rejected as streams are not supported; {} → {}.",
            sql,
            select.getModifierNode(SqlSelectKeyword.STREAM));
        throw new QueryBuildingException("SQL streams are not supported.");
      }

      LOG.trace("- ModNod-ALL: {}", select.getModifierNode(SqlSelectKeyword.ALL));
      LOG.trace("- ModNod-DISTINCT: {}", select.getModifierNode(SqlSelectKeyword.DISTINCT));

      LOG.trace("- SelectList: {}", select.getSelectList());
      LOG.trace("- From: {}", select.getFrom());
      LOG.trace("- Where: {}", select.getWhere());
      LOG.trace("- Group: {}", select.getGroup());

      if (select.getGroup() != null && select.getModifierNode(SqlSelectKeyword.DISTINCT) != null) {
        LOG.warn(
            "Rejected as distinct clauses are not supported alongside group by clauses; {}.", sql);
        throw new QueryBuildingException("SQL DISTINCT clauses cannot be combined with GROUP BY.");
      }

      if (select.getHaving() != null) {
        LOG.warn("Rejected as having clauses are not supported; {} → {}.", sql, select.getHaving());
        throw new QueryBuildingException("SQL HAVING clauses are not supported.");
      }

      LOG.trace("- WindowList: {}", select.getWindowList());

      if (select.getQualify() != null) {
        LOG.warn("SQL qualify clauses are not supported; {} → {}.", sql, select.getQualify());
        throw new QueryBuildingException("SQL QUALIFY clauses are not supported.");
      }

      LOG.trace("- OrderList: {}", select.getOrderList());
      LOG.trace("- Fetch: {}", select.getFetch());
      LOG.trace("- Offset: {}", select.getOffset());
      // LOG.trace("- OpList: " + select.getOperandList());

      for (SqlNode n : select.getSelectList().getList()) {
        if (n instanceof SqlIdentifier) {
          SqlIdentifier id = (SqlIdentifier) n;
          if (id.isStar()) {
            LOG.warn("Rejected as star selects are not supported; {} → {}.", sql, id);
            throw new QueryBuildingException("Star selects are not supported.");
          }
        }
      }

      Map<SqlKind, Integer> count = select.accept(new KindValidatorAndCounterVisitor());
      LOG.debug("Count: {}", count);
      if (count.getOrDefault(SqlKind.SELECT, -1) != 1) {
        LOG.warn("Rejected as multiple selects present; {} → {}.", sql);
        throw new QueryBuildingException("Must be exactly one SQL select statement.");
      }

      if (count.getOrDefault(SqlKind.JOIN, 0) > 0) {
        LOG.warn("Rejected as joins present; {} → {}.", sql);
        throw new QueryBuildingException("Joins are not supported.");
      }

      if (count.getOrDefault(SqlKind.ARRAY_VALUE_CONSTRUCTOR, 0) > 0) {
        LOG.warn("Rejected as arrays must use array('value') syntax; {} → {}.", sql);
        throw new QueryBuildingException("Array constructors should use array(), not array[].");
      }

      // Validate WKT strings.
      select.accept(new GeometryPointCounterVisitor());

      // Prepend catalog if defined
      if (catalog != null) {
        String firstTable = rootSchema.getTableNames().stream().findFirst().get();
        Prepare.PreparingTable table =
            catalogReader.getTable(Collections.singletonList(firstTable));
        select.setFrom(
            ((SqlSelect)
                    sqlParser.parseQuery(
                        "SELECT "
                            + table.getRowType().getFieldList().get(0).getName()
                            + " FROM "
                            + catalog
                            + "."
                            + firstTable))
                .getFrom());
      }

      return select;
    } catch (CalciteContextException cce) {
      // Just the main message is decent.
      throw new QueryBuildingException(cce.getMessage(), cce);
    } catch (SqlParseException spe) {
      // Use the first line only, otherwise there's a huge list of all possible keywords.
      throw new QueryBuildingException(spe.getMessage().split("\n")[0], spe);
    } catch (KindValidatorAndCounterVisitor.SqlValidationException sve) {
      // Our custom reasons for invalidation.
      throw new QueryBuildingException(sve.getMessage(), sve);
    }
  }

  public SqlDialect getDialect() {
    return dialect;
  }
}
