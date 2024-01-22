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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Table definition for testing
 */
class TestOccurrenceTable extends AbstractTable {

  private final String tableName;

  public TestOccurrenceTable(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    builder.add("gbifid", SqlTypeName.INTEGER);
    builder.add("datasetkey", SqlTypeName.CHAR);
    builder.add("countrycode", SqlTypeName.CHAR);
    builder.add("specieskey", SqlTypeName.INTEGER);
    builder.add("eventdate", SqlTypeName.TIMESTAMP);
    builder.add("year", SqlTypeName.INTEGER);
    builder.add("month", SqlTypeName.INTEGER);
    builder.add("day", SqlTypeName.INTEGER);
    builder.add("decimallatitude", SqlTypeName.DOUBLE);
    builder.add("decimallongitude", SqlTypeName.DOUBLE);
    builder.add("coordinateuncertaintyinmeters", SqlTypeName.DOUBLE);
    builder.add("occurrencestatus", SqlTypeName.CHAR);
    builder.add("identificationverificationstatus", SqlTypeName.CHAR);
    builder.add("hascoordinate", SqlTypeName.BOOLEAN);

    RelDataTypeFactory tdf = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType issue = tdf.createArrayType(tdf.createSqlType(SqlTypeName.CHAR), -1);
    builder.add("issue", issue);
    return builder.build();
  }

  public String getTableName() {
    return tableName;
  }

  public List<SqlOperator> additionalOperators() {
    List<SqlOperator> list = new ArrayList<>();

    list.add(new SqlFunction("stringArrayContains",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.BOOLEAN,
      null,
      OperandTypes.family(SqlTypeFamily.ARRAY, SqlTypeFamily.CHARACTER, SqlTypeFamily.BOOLEAN),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    list.add(new SqlFunction("eeaCellCode",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.CHAR,
      null,
      OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
      SqlFunctionCategory.USER_DEFINED_FUNCTION));

    return list;
  }
}
