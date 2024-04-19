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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;

import lombok.experimental.UtilityClass;

@UtilityClass
public class SqlValidatorTestUtil {

  public static HiveSqlValidator createOccurrenceTableValidator(String catalog) {

    SchemaPlus rootSchema = Frameworks.createRootSchema(true);

    TestOccurrenceTable testTable = new TestOccurrenceTable("occurrence");
    rootSchema.add(
        catalog,
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            // Define a map to hold tables
            Map<String, Table> tables = new HashMap<>();
            // Add your table to the map
            tables.put("occurrence", testTable);
            return tables;
          }
        });
    rootSchema.add(testTable.getTableName(), testTable);

    return new HiveSqlValidator(rootSchema, testTable.additionalOperators());
  }

  public static HiveSqlValidator createOccurrenceTableValidator() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    TestOccurrenceTable testTable = new TestOccurrenceTable("occurrence");
    rootSchema.add(testTable.getTableName(), testTable);

    return new HiveSqlValidator(rootSchema, testTable.additionalOperators());
  }
}
