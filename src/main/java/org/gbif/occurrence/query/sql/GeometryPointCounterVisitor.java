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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Counts parts of SQL queries.
 *
 * Throws a RuntimeException if it encounters invalid WKT.
 */
class GeometryPointCounterVisitor implements SqlVisitor<Integer> {

  public static final String GBIF_WITHIN = "gbif_within";

  // Whether we are inside a GBIF_WITHIN function.
  private int within = 0;

  @Override
  public Integer visit(SqlCall call) {
    int c = 0;
    if (call.getOperator().isName(GBIF_WITHIN, false)) {
      within++;
    }
    for (SqlNode n : call.getOperandList()) {
      if (n != null) c += n.accept(this);
    }
    if (call.getOperator().isName(GBIF_WITHIN, false)) {
      within--;
    }
    return c;
  }

  @Override
  public Integer visit(SqlNodeList nodeList) {
    int c = 0;
    for (SqlNode n : nodeList.getList()) {
      if (n != null) c += n.accept(this);
    }
    return c;
  }

  @Override
  public Integer visit(SqlLiteral literal) {
    if (within > 0) {
      try {
        Geometry geometry = new WKTReader().read(literal.toValue());
        return geometry.getNumPoints();
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }
    return 0;
  }

  @Override
  public Integer visit(SqlIdentifier id) {
    return 0;
  }

  @Override
  public Integer visit(SqlDataTypeSpec type) {
    return 0;
  }

  @Override
  public Integer visit(SqlDynamicParam param) {
    return 0;
  }

  @Override
  public Integer visit(SqlIntervalQualifier intervalQualifier) {
    return 0;
  }

  @Override
  public Integer visitNode(SqlNode n) {
    return n.accept(this);
  }
}
