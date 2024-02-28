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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlCounterTest {

  HiveSqlValidator hiveSqlValidator;

  SqlCounterTest() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    TestOccurrenceTable testTable = new TestOccurrenceTable("occurrence");
    rootSchema.add(testTable.getTableName(), testTable);

    hiveSqlValidator = new HiveSqlValidator(rootSchema, testTable.additionalOperators());
  }

  @Test
  public void testSqlCount() {
    // AND/ORs should count one each.
    // Literals should count one each.
    final String sql = "SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode IN('DK', 'FO', 'GL') AND speciesKey = 1 OR speciesKey = 2 GROUP BY datasetkey ORDER BY datasetkey LIMIT 10 OFFSET 20";

    HiveSqlQuery q = new HiveSqlQuery(hiveSqlValidator, sql);
    assertEquals(3+1+1+1+1+1+1, q.getPredicateCount());
  }

  @Test
  public void testSqlWithinCount() {
    String withinSql = "SELECT gbifid FROM occurrence WHERE gbif_within('POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))', decimalLatitude, decimalLongitude)";
    HiveSqlQuery q = new HiveSqlQuery(hiveSqlValidator, withinSql);
    assertEquals(1, q.getPredicateCount());
    assertEquals(5, q.getPointsCount());

    withinSql = "SELECT gbifid FROM occurrence WHERE gbif_within('POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))', decimalLatitude, decimalLongitude) OR gbif_within('POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))', decimalLatitude, decimalLongitude)";
    q = new HiveSqlQuery(hiveSqlValidator, withinSql);
    assertEquals(1+1+1, q.getPredicateCount());
    assertEquals(5+5, q.getPointsCount());
  }

  @Test
  public void testCountNull() {
    HiveSqlQuery q = new HiveSqlQuery(hiveSqlValidator, "SELECT gbifid FROM occurrence");
    assertEquals(0, q.getPredicateCount());
  }
}
