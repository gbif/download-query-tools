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

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Testing validation queries.
 */
public class HiveSqlValidatorTest {

  HiveSqlValidator hiveSqlValidator;

  HiveSqlValidatorTest() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    TestOccurrenceTable testTable = new TestOccurrenceTable("occurrence");
    rootSchema.add(testTable.getTableName(), testTable);

    hiveSqlValidator = new HiveSqlValidator(rootSchema, testTable.additionalOperators());
  }

  @ParameterizedTest
  @MethodSource("provideStringsForAllowedSql")
  public void testAllowedSql(String sql) {
    hiveSqlValidator.validate(sql);
  }

  @ParameterizedTest
  @MethodSource("provideStringsForForbiddenSql")
  public void testForbiddenSql(String sql) {
    try {
      hiveSqlValidator.validate(sql);
      fail();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  private static Stream<Arguments> provideStringsForAllowedSql() {
    return Stream.of(
      Arguments.of("SELECT " +
        "  \"year\", " +
        "  eeaCellCode(1000, decimallatitude, decimallongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS eeaCellCode, " +
        "  speciesKey, " +
        "  COUNT(*) AS n, " +
        "  MIN(COALESCE(coordinateUncertaintyInMeters, 1000)) AS minCoordinateUncertaintyInMeters " +
        "FROM occurrence " +
        "WHERE " +
        "  occurrenceStatus = 'PRESENT' " +
        "  AND speciesKey IS NOT NULL " +
        "  AND NOT stringArrayContains(issue, 'ZERO_COORDINATE', false) " +
        "  AND NOT stringArrayContains(issue, 'COORDINATE_OUT_OF_RANGE', false) " +
        "  AND NOT stringArrayContains(issue, 'COORDINATE_INVALID', false) " +
        "  AND NOT stringArrayContains(issue, 'COUNTRY_COORDINATE_MISMATCH', false) " +
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
        "ORDER BY \"year\" DESC, eeaCellCode ASC, speciesKey ASC"),

      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey"),
      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey ORDER BY datasetkey"),
      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey LIMIT 10 OFFSET 20"),
      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey ORDER BY datasetkey LIMIT 10 OFFSET 20"),

      // Probably not what was intended, stricter AS?
      Arguments.of("SELECT countrycode datasetkey FROM occurrence"));
  }

  private static Stream<Arguments> provideStringsForForbiddenSql() {
    return Stream.of(
      // Block subselect
      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence " +
        "WHERE countryCode IN (SELECT DISTINCT countryCode FROM occurrence WHERE \"month\" = 12)"),
      // Block having
      Arguments.of("SELECT datasetkey, COUNT(*) FROM occurrence " +
        "GROUP BY countrycode " +
        "HAVING COUNT(*) > 2"),
      // Block partitioning
      Arguments.of("SELECT datasetkey, COUNT(DISTINCT \"day\") FROM occurrence " +
        "GROUP BY datasetkey " +
        "OVER (PARTITION BY \"month\" ORDER BY datasetkey) = 1"),
      Arguments.of("SELECT datasetkey, COUNT(DISTINCT \"day\") FROM occurrence " +
        "GROUP BY datasetkey " +
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY \"month\" ORDER BY datasetkey) = 1"),
      // Block query hints
      Arguments.of("SELECT /*+ MAPJOIN(time_dim) */ datasetkey FROM occurrence"),
      // Block joins
      Arguments.of("SELECT o1.datasetkey, COUNT(DISTINCT o1.\"month\") FROM occurrence o1 INNER JOIN occurrence o2 ON o1.datasetkey = o2.datasetkey " +
        "GROUP BY o1.datasetkey"),
      Arguments.of("SELECT o1.datasetkey, COUNT(DISTINCT o1.\"month\") FROM occurrence o1, occurrence o2 " +
        "WHERE o1.datasetkey = o2.datasetkey " +
        "GROUP BY o1.datasetkey"),
      Arguments.of("SELECT o1.datasetkey, COUNT(DISTINCT o1.\"month\") FROM occurrence o1, occurrence o2 " +
        "GROUP BY o1.datasetkey"),
      // Block * select
      Arguments.of("SELECT o1.* FROM occurrence o1"),
      // Block with
      Arguments.of("WITH subquery AS (SELECT DISTINCT datasetkey FROM occurrence) SELECT sq.datasetkey FROM subquery sq"),
      Arguments.of("WITH subquery AS (SELECT DISTINCT datasetkey FROM occurrence) SELECT sq.datasetkey FROM subquery sq, occurrence o"),
      // Block modification of data
      Arguments.of("DELETE FROM occurrence"),
      Arguments.of("INSERT INTO occurrence (datasetkey, countrycode, \"month\", \"day\") VALUES ('aoeuaoeuaoeu', 'XX', 0, 0)"),
      Arguments.of("UPDATE occurrence SET datasetkey = 'AOEU'"),
      Arguments.of("TRUNCATE occurrence"),
      Arguments.of("DROP TABLE occurrence"),

      // Block incorrect table and column names, commands and syntax
      Arguments.of("SELECT datasetkey FROM wrongTable occurrence"),
      Arguments.of("SELECT wrongColumn FROM occurrence"),
      Arguments.of("WRONGCOMMAND datasetkey FROM occurrence"),
      Arguments.of("FROM occurrence SELECT datasetkey"),
      Arguments.of("SELECT datasetkey FROM occurrence; SELECT datasetkey FROM occurrence"));

      // TODO: Block GROUP BY gbifid
      //Arguments.of("SELECT gbifid FROM occurrence GROUP BY gbifid"),
      // TODO: Block ORDER BY gbifid
      //Arguments.of("SELECT gbifid FROM occurrence ORDER BY gbifid"),
  }
}
