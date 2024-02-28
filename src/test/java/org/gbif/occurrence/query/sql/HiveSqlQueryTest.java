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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HiveSqlQueryTest {

  HiveSqlValidator hiveSqlValidator;

  HiveSqlQueryTest() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    TestOccurrenceTable testTable = new TestOccurrenceTable("occurrence");
    rootSchema.add(testTable.getTableName(), testTable);

    hiveSqlValidator = new HiveSqlValidator(rootSchema, testTable.additionalOperators());
  }

  @ParameterizedTest
  @MethodSource("provideSql")
  public void testAllowedSql(String sql, String where, List<String> columns) {
    HiveSqlQuery q = new HiveSqlQuery(hiveSqlValidator, sql);

    assertEquals(where, q.getSqlWhere());
    assertArrayEquals(columns.toArray(), q.getSqlSelectColumnNames().toArray());
  }

  private static Stream<Arguments> provideSql() {
    return Stream.of(
      Arguments.of("SELECT " +
        "  \"year\", " +
        "  gbif_eeaCellCode(1000, decimallatitude, decimallongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS eeaCellCode, " + // with AS
        "  occurrence.speciesKey, " +
        "  COUNT(*), " + // no AS
        "  MIN(COALESCE(coordinateUncertaintyInMeters, 1000)) AS \"minCoordinateUncertaintyInMeters\" " + // keeping case
        "FROM occurrence " +
        "WHERE " +
        "  occurrenceStatus = 'PRESENT' " +
        "  AND speciesKey IS NOT NULL " +
        "  AND NOT array_contains(issue, 'ZERO_COORDINATE') " +
        "  AND NOT array_contains(issue, 'COORDINATE_OUT_OF_RANGE') " +
        "  AND NOT array_contains(issue, 'COORDINATE_INVALID') " +
        "  AND NOT array_contains(issue, 'COUNTRY_COORDINATE_MISMATCH') " +
        "  AND (identificationVerificationStatus IS NULL " +
        "    OR NOT (LOWER(identificationVerificationStatus) LIKE '%unverified%' " +
        "         OR LOWER(identificationVerificationStatus) LIKE '%unvalidated%' " +
        "         OR LOWER(identificationVerificationStatus) LIKE '%not able to validate%' " +
        "         OR LOWER(identificationVerificationStatus) LIKE '%control could not be conclusive due to insufficient knowledge%' " +
        "         OR LOWER(identificationVerificationStatus) LIKE '%unconfirmed%' " +
        "         OR LOWER(identificationVerificationStatus) LIKE '%unconfirmed - not reviewed%' " +
        "         OR LOWER(identificationVerificationStatus) LIKE '%validation requested%')) " +
        "  AND countryCode = 'SI' " +
        "  AND \"year\" > 1000 " +
        "  AND hasCoordinate " +
        "GROUP BY " +
        "  \"year\", " +
        "  eeaCellCode, " +
        "  speciesKey " +
        "ORDER BY \"year\" DESC, eeaCellCode ASC, speciesKey ASC",
        ("occurrence.occurrencestatus    = 'PRESENT' " +
        "  AND occurrence.specieskey IS NOT NULL " +
        "  AND NOT ARRAY_CONTAINS(occurrence.issue, 'ZERO_COORDINATE') " +
        "  AND NOT ARRAY_CONTAINS(occurrence.issue, 'COORDINATE_OUT_OF_RANGE') " +
        "  AND NOT ARRAY_CONTAINS(occurrence.issue, 'COORDINATE_INVALID') " +
        "  AND NOT ARRAY_CONTAINS(occurrence.issue, 'COUNTRY_COORDINATE_MISMATCH') " +
        "  AND (occurrence.identificationverificationstatus IS NULL " +
        "    OR NOT (LOWER(occurrence.identificationverificationstatus) LIKE '%unverified%' " +
        "         OR LOWER(occurrence.identificationverificationstatus) LIKE '%unvalidated%' " +
        "         OR LOWER(occurrence.identificationverificationstatus) LIKE '%not able to validate%' " +
        "         OR LOWER(occurrence.identificationverificationstatus) LIKE '%control could not be conclusive due to insufficient knowledge%' " +
        "         OR LOWER(occurrence.identificationverificationstatus) LIKE '%unconfirmed%' " +
        "         OR LOWER(occurrence.identificationverificationstatus) LIKE '%unconfirmed - not reviewed%' " +
        "         OR LOWER(occurrence.identificationverificationstatus) LIKE '%validation requested%')) " +
        "  AND occurrence.countrycode = 'SI' " +
        "  AND occurrence.year > 1000 " +
        "  AND occurrence.hascoordinate").replaceAll(" +", " "),
        Arrays.asList("year", "eeacellcode", "specieskey", "COUNT(*)", "minCoordinateUncertaintyInMeters")),

      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey",
        "occurrence.countrycode = 'DK' AND occurrence.month > occurrence.day",
        Arrays.asList("datasetkey", "COUNT(*)")),

      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey ORDER BY datasetkey",
        "occurrence.countrycode = 'DK' AND occurrence.month > occurrence.day",
        Arrays.asList("datasetkey", "COUNT(*)")),

      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey LIMIT 10 OFFSET 20",
        "occurrence.countrycode = 'DK' AND occurrence.month > occurrence.day",
        Arrays.asList("datasetkey", "COUNT(*)")),

      Arguments.of("SELECT datasetkey, gbifid > 10000, -gbifid, TRUE, 5, countrycode IS NULL, " +
          "gbifid * 2, round(decimallatitude, 1), hour(eventdate), CAST(gbifid AS char), " +
          "gbif_eeaCellCode(1000, decimallatitude, decimallongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) " +
          "FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\"",
        "occurrence.countrycode = 'DK' AND occurrence.month > occurrence.day",
        Arrays.asList("datasetkey", "gbifid > 10000", "- gbifid", "TRUE", "5", "countrycode IS NULL", "gbifid * 2",
          "ROUND(decimallatitude, 1)", "EXTRACT(HOUR FROM eventdate)", "CAST(gbifid AS CHAR)",
          "GBIF_EEACELLCODE(1000, decimallatitude, decimallongitude, CASE WHEN coordinateuncertaintyinmeters IS NOT NULL THEN coordinateuncertaintyinmeters ELSE 1000 END)")),

      // No where clause
      Arguments.of("SELECT gbifid FROM occurrence",
        "1 = 1",
        Arrays.asList("gbifid"))
    );
  }
}
