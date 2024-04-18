package org.gbif.occurrence.query.sql;

import lombok.experimental.UtilityClass;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;

import java.util.HashMap;
import java.util.Map;

@UtilityClass
public class SqlValidatorTestUtil {

    public static HiveSqlValidator createOccurrenceTableValidator(String catalog) {

        SchemaPlus rootSchema = Frameworks.createRootSchema(true);

        TestOccurrenceTable testTable = new TestOccurrenceTable("occurrence");
        rootSchema.add(catalog, new AbstractSchema() {
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
