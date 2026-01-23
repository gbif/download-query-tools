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

import org.gbif.api.exception.QueryBuildingException;

import java.util.stream.Stream;

import org.apache.calcite.sql.SqlSelect;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Testing validation queries.
 */
public class HiveSqlValidatorTest {

  HiveSqlValidator hiveSqlValidator;

  private static final String TEST_CATALOG = "cattest";

  HiveSqlValidatorTest() {
    hiveSqlValidator = SqlValidatorTestUtil.createOccurrenceTableValidator();
  }

  @ParameterizedTest
  @MethodSource("provideStringsForAllowedSql")
  public void testAllowedSql(String sql) throws Exception {
    hiveSqlValidator.validate(sql);
  }

  @ParameterizedTest
  @MethodSource("provideStringsForAllowedSql")
  public void testAllowedSqlInCatalog(String sql) throws Exception {
    HiveSqlValidator catalogValidator =
        SqlValidatorTestUtil.createOccurrenceTableValidator(TEST_CATALOG);
    catalogValidator.validate(sql, TEST_CATALOG);
  }

  /**
   * Ensure aliases "occurrence AS occ" aren't messed up by adding the Iceberg catalogue.
   */
  @ParameterizedTest
  @MethodSource("provideStringsForAsOperator")
  public void testAsOperator(String sql, String expectedFragment) throws Exception {
    HiveSqlValidator catalogValidator =
      SqlValidatorTestUtil.createOccurrenceTableValidator(TEST_CATALOG);
    SqlSelect select = catalogValidator.validate(sql, TEST_CATALOG);
    assertTrue(select.toSqlString(catalogValidator.getDialect()).toString().contains(expectedFragment));
  }

  /**
   * Check support exists for appropriate Hive functions
   * https://cwiki.apache.org/confluence/display/hive/languagemanual+udf
   */
  @ParameterizedTest
  @MethodSource("provideStringsForHiveBuiltInFunctions")
  public void testHiveBuiltInFunctions(String sql) throws Exception {
    hiveSqlValidator.validate(sql);
  }

  @ParameterizedTest
  @MethodSource("provideStringsForForbiddenSql")
  public void testForbiddenSql(String sql) {
    try {
      hiveSqlValidator.validate(sql);
      fail();
    } catch (QueryBuildingException e) {
      System.out.println(e.getMessage());
    }
  }

  @ParameterizedTest
  @MethodSource("provideStringsForBlockedHiveFunctions")
  public void testForbiddenSql(String sql, String expectedErrorFragment) {
    try {
      hiveSqlValidator.validate(sql);
      fail();
    } catch (QueryBuildingException e) {
      if (!e.getMessage().contains(expectedErrorFragment)) {
        System.out.println(e.getMessage());
      }
      assertTrue(e.getMessage().contains(expectedErrorFragment));
    }
  }

  @ParameterizedTest
  @MethodSource("provideStringsForBadSql")
  public void testValidateSql(String sql, String expectedErrorFragment) {
    try {
      hiveSqlValidator.validate(sql);
      fail();
    } catch (QueryBuildingException e) {
      if (!e.getMessage().contains(expectedErrorFragment)) {
        System.out.println(e.getMessage());
      }
      assertTrue(e.getMessage().contains(expectedErrorFragment));
    }
  }

  private static Stream<Arguments> provideStringsForAllowedSql() {
    return Stream.of(
        Arguments.of(
            "SELECT "
                + "  \"year\", "
                + "  gbif_eeaCellCode(1000, decimallatitude, decimallongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS eeaCellCode, "
                + "  familyKey, "
                + "  speciesKey, "
                + "  COUNT(*) AS n, "
                + "  MIN(COALESCE(coordinateUncertaintyInMeters, 1000)) AS minCoordinateUncertaintyInMeters, "
                + "  IF(ISNULL(familyKey), NULL, SUM(COUNT(*)) OVER (PARTITION BY familyKey)) AS familyCount "
                + "FROM occurrence "
                + "WHERE "
                + "  occurrenceStatus = 'PRESENT' "
                + "  AND speciesKey IS NOT NULL "
                + "  AND NOT array_contains(issue, 'ZERO_COORDINATE') "
                + "  AND NOT array_contains(issue, 'COORDINATE_OUT_OF_RANGE') "
                + "  AND NOT array_contains(issue, 'COORDINATE_INVALID') "
                + "  AND NOT array_contains(issue, 'COUNTRY_COORDINATE_MISMATCH') "
                + "  AND (identificationVerificationStatus IS NULL "
                + "    OR NOT (LOWER(identificationVerificationStatus) LIKE '%unverified%' "
                + "         OR LOWER(identificationVerificationStatus) LIKE '%unvalidated%' "
                + "         OR LOWER(identificationVerificationStatus) LIKE '%not able to validate%' "
                + "         OR LOWER(identificationVerificationStatus) LIKE '%control could not be conclusive due to insufficient knowledge%' "
                + "         OR LOWER(identificationVerificationStatus) LIKE '%unconfirmed%' "
                + "         OR LOWER(identificationVerificationStatus) LIKE '%unconfirmed - not reviewed%' "
                + "         OR LOWER(identificationVerificationStatus) LIKE '%validation requested%')) "
                + "  AND countryCode = 'SI' "
                + "  AND \"year\" > 1000 "
                + "  AND hasCoordinate "
                + "GROUP BY "
                + "  \"year\", "
                + "  eeaCellCode, "
                + "  familyKey, "
                + "  speciesKey "
                + "ORDER BY \"year\" DESC, eeaCellCode ASC, speciesKey ASC"),
        Arguments.of(
            "SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey"),
        Arguments.of(
            "SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey ORDER BY datasetkey"),
        Arguments.of(
            "SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey LIMIT 10 OFFSET 20"),
        Arguments.of(
            "SELECT datasetkey, COUNT(*) FROM occurrence WHERE countryCode = 'DK' and \"month\" > \"day\" GROUP BY datasetkey ORDER BY datasetkey LIMIT 10 OFFSET 20"),
        Arguments.of(
            "SELECT gbifid FROM occurrence WHERE gbif_within('POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))', decimalLatitude, decimalLongitude)"),
        Arguments.of(
            "SELECT DISTINCT datasetkey FROM occurrence WHERE array_contains(issue, 'COORDINATE_INVALID')"),

        // Windowing/partitioning needed for B-cubed queries.
        Arguments.of(
            "SELECT datasetkey, COUNT(DISTINCT \"day\"), SUM(COUNT(*)) OVER (PARTITION BY \"month\" ORDER BY datasetkey) "
                + "FROM occurrence "
                + "GROUP BY datasetkey, \"month\""),

        // Vocabulary (struct) fields
        Arguments.of(
            "SELECT lifestage.concept, lifestage.lineage, COUNT(*) "
                + "FROM occurrence WHERE lifestage.concept IS NOT NULL "
                + "GROUP BY concept, lineage"),

        // Cope with semicolons at line endings.
        Arguments.of("SELECT DISTINCT datasetkey FROM occurrence; ;; ;\t\t;\t"),

        // Probably not what was intended, stricter AS?
        Arguments.of("SELECT countrycode datasetkey FROM occurrence"),

        // Cope with non-ASCII query parameters
        Arguments.of("SELECT datasetkey FROM occurrence WHERE location LIKE 'København' OR location LIKE '沖縄';"));
  }

  private static Stream<Arguments> provideStringsForHiveBuiltInFunctions() {
    return Stream.of(
        // Relational operators
        // Not included: gbifid == gbifid, gbifid <=> gbifid, gbifid RLIKE 'x', gbifid REGEXP 'x'
        Arguments.of(
            "SELECT gbifid = gbifid, gbifid <> gbifid, \n"
                + "gbifid != gbifid, gbifid < gbifid, gbifid <= gbifid, gbifid > gbifid, gbifid >= gbifid, \n"
                + "gbifid NOT BETWEEN 1 AND 2, gbifid IS NULL, gbifid IS NOT NULL, TRUE IS TRUE, gbifid LIKE 'abcd%' \n"
                + " FROM occurrence;"),

        // Arithmetic operators
        // Not included: 1 DIV 1, 1 & 1, 1 | 1, 1 ^ 1, ~1
        Arguments.of("SELECT 1 + 1, 1 - 1, 1 * 1, 1 / 1, 1 % 1 FROM occurrence;"),

        // Logical operators
        // Not included: ! TRUE,
        Arguments.of(
            "SELECT TRUE AND TRUE, TRUE OR TRUE, NOT TRUE, gbifid IN('1234', '5678') FROM occurrence;"),

        // String operators
        Arguments.of("SELECT 'a' || 'b' FROM occurrence;"),

        // Complex type constructors
        Arguments.of(
            "SELECT "
                +
                // "MAP('x', 'y') " +
                // "STRUCT('x', 'y'), " +
                // "NAMED_STRUCT('x', 'y'), " +
                "ARRAY('a') "
                +
                // "CREATE_UNION('a', 'b') " +
                "FROM occurrence;"),

        // Operators on complex types (not done: map[key])
        Arguments.of("SELECT issue[0], lifestage.concept FROM occurrence;"),

        // Mathematical functions
        // Not included: WIDTH_BUCKET(10, 50, 80, 15)
        // Not our Hive: BROUND(decimalLatitude), BROUND(decimalLatitude, 2),
        Arguments.of(
            "SELECT ROUND(decimalLatitude), ROUND(decimalLatitude, 2), \n"
                + "FLOOR(decimalLatitude), ceil(decimalLatitude), CEILING(decimalLatitude), \n"
                + "RAND(), RAND(1234), EXP(2.0), LN(2.7), LOG10(100), LOG2(64), POWER(2, 3), SQRT(81), HEX(63), \n"
                + "UNHEX('A'), CONV(32, 10, 16), CONV('32', 10, 16), ABS(-1), SIN(3.141/2), ASIN(0.5), COS(3.141/2), ACOS(0.5), \n"
                + "TAN(3.141/2), ATAN(0.5), DEGREES(3.141/2), RADIANS(180), POSITIVE(-1), NEGATIVE(1), SIGN(-1), E(), PI(), \n"
                + "FACTORIAL(6), CBRT(27), SHIFTLEFT(2, 2), SHIFTRIGHT(2, 2), SHIFTRIGHTUNSIGNED(2, 2), GREATEST(1, 2), \n"
                + "LEAST(1, 2) FROM occurrence"),

        // Collection functions (TODO: Maps)
        Arguments.of(
            "SELECT size(issue), array_contains(issue, 'A'), sort_array(issue) FROM occurrence"),

        // Type conversion functions
        // Not included: BINARY('A'),
        Arguments.of("SELECT CAST('1' AS BIGINT) FROM occurrence"),

        // Date functions
        Arguments.of(
            "SELECT FROM_UNIXTIME(0), FROM_UNIXTIME(0, 'uuuu-MM-dd'), UNIX_TIMESTAMP(), \n"
                + "UNIX_TIMESTAMP('2024-03-05 15:13:00'), UNIX_TIMESTAMP('2024-03-05', 'uuuu-MM-dd'), TO_DATE('2024-03-05 15:13:00'), \n"
                + "YEAR(CAST('2024-03-05 15:13:00' AS DATE)), QUARTER(CAST('2024-03-05 15:13:00' AS DATE)), MONTH(CAST('2024-03-05 15:13:00' AS DATE)), \n"
                + "DAYOFMONTH(CAST('2024-03-05 15:13:00' AS DATE)), HOUR(CAST('2024-03-05 15:13:00' AS DATE)), \n"
                + "MINUTE(CAST('2024-03-05 15:13:00' AS DATE)), SECOND(CAST('2024-03-05 15:13:00' AS DATE)), WEEKOFYEAR(CAST('2024-03-05 15:13:00' AS DATE)), \n"
                + "EXTRACT(month FROM CAST('2024-03-05' AS DATE)), "
                + "DATEDIFF('2024-03-05 15:13:00', '2024-03-05 15:13:00'), \n"
                + "DATE_ADD('2024-03-05 15:13:00', 1), DATE_SUB('2024-03-05 15:13:00', 1), FROM_UTC_TIMESTAMP(1, 'UTC'), \n"
                + "TO_UTC_TIMESTAMP('2024-03-05 15:13:00', 'Z'), CURRENT_DATE(), CURRENT_TIMESTAMP(), ADD_MONTHS('2024-03-05 15:13:00', 1), \n"
                + "LAST_DAY(CAST('2024-03-05' AS DATE)), NEXT_DAY('2024-03-05 15:13:00', 'TU'), TRUNC('2024-03-05 15:13:00', 'MM'), \n"
                + "MONTHS_BETWEEN('2024-03-05 15:13:00', '2024-03-05 15:13:00'), DATE_FORMAT('2024-03-05 15:13:00', 'D') FROM occurrence"),

        // Conditional functions
        Arguments.of(
            "SELECT IF(true, 1, 2), ISNULL(1), ISNOTNULL(1), NVL(1, 2), COALESCE(NULL, 1), \n"
                + "CASE gbifid WHEN '1' THEN 2 ELSE 3 END, CASE WHEN TRUE THEN 2 ELSE 3 END, NULLIF(1, 2), \n"
                + "ASSERT_TRUE(true) FROM occurrence"),

        // String functions
        // Not included: CONTEXT_NGRAMS(...), NGRAMS()
        Arguments.of(
            "SELECT ASCII('x'), BASE64('x'), CHARACTER_LENGTH('X'), CHR(60), CONCAT('A', 'B'), "
                + "CONCAT_WS('-', 'A', 'B'), CONCAT_WS(' ', issue), DECODE('A', 'UTF-8'), ELT(2,'HELLO','WORLD'), "
                + "ENCODE('A', 'UTF-8'), FIELD('WORLD', 'SAY', 'HELLO', 'WORLD'), FIND_IN_SET('ab', 'abc,b,ab,c,def'), "
                + "FORMAT_NUMBER(1000, '#,###'), GET_JSON_OBJECT('{}', '.'), IN_FILE('A', '/A/B'), INSTR('A', 'CAT'), "
                + "LENGTH('AOEU'), LOCATE('A', 'ABCDABCD', 2), LOWER('AOEU'), LPAD('A', 1, ' '), LTRIM('A '), "
                + "OCTET_LENGTH('AOEU'), PARSE_URL('a', 'a', 'a'), PRINTF('Y', 'X'), QUOTE('A'), REGEXP_EXTRACT('abc', 'a(b)c', 1), "
                + "REGEXP_REPLACE('abc', 'b', ''), REPEAT('a', 3), REPLACE('a', 'a', 'b'), REVERSE('abc'), RPAD('s', 1, ' '), "
                + "RTRIM(' a '), SPACE(4), SPLIT('a,b', ','), SUBSTR('abc', 1), SUBSTRING('abc', 1), SUBSTRING_INDEX('abc', 'b', 1), "
                + "TRANSLATE('a', 'b', 'c'), TRIM(' a '), UNBASE64('6'), UPPER('abcd'), INITCAP('ab cd'), LEVENSHTEIN('cat', 'mat'),"
                + "SOUNDEX('mouse') FROM occurrence;"),

        // Data masking functions
        Arguments.of(
            "SELECT MASK('a', 'X', 'x', '#'), MASK_FIRST_N('a'), MASK_LAST_N('b'), "
                + "MASK_SHOW_FIRST_N('a'), MASK_SHOW_LAST_N('B'), MASK_HASH('A') FROM occurrence"),

        // Misc functions
        Arguments.of(
            "SELECT HASH('x'), MD5('A'), SHA1('A'), SHA2('A'), CRC32('A'), "
                + "AES_ENCRYPT('A', 'A'), AES_DECRYPT('A', 'B') FROM occurrence"),

        // Aggregate functions.
        Arguments.of(
            "SELECT "
                + "COUNT(datasetkey), "
                + "COUNT(DISTINCT datasetkey, decimallatitude), "
                + "SUM(decimallatitude), "
                + "SUM(DISTINCT decimallatitude), "
                + "AVG(decimallatitude), "
                + "AVG(DISTINCT decimallatitude), "
                + "MIN(decimallatitude), "
                + "MAX(decimallatitude), "
                + "VARIANCE(decimallatitude), "
                + "VAR_SAMP(decimallatitude), "
                + "STDDEV_POP(decimallatitude), "
                + "STDDEV_SAMP(decimallatitude), "
                + "COVAR_POP(decimallatitude, decimallongitude), "
                + "COVAR_SAMP(decimallatitude, decimallongitude) "
                // + "CORR(decimallatitude, decimallongitude), "
                // + "PERCENTILE(\"year\", array(25, 50, 75)), "
                // + "PERCENTILE_APPROX(decimallatitude, 50), "
                // + "PERCENTILE_APPROX(decimallatitude, array(25, 50, 75)), "
                // + "HISTOGRAM_NUMERIC(decimallatitude, 5), "
                // + "COLLECT_SET(decimallatitude), "
                // + "COLLECT_LIST(decimallatitude), "
                // + "NTILE(5) "
                + "FROM occurrence"));
  }

  private static Stream<Arguments> provideStringsForAsOperator() {
    return Stream.of(
      Arguments.of(
        "SELECT gbifid FROM occurrence WHERE countrycode = 'CL'",
        "FROM cattest.occurrence\nWHERE occurrence.countrycode = 'CL'"),
      Arguments.of(
        "SELECT gbifid FROM occurrence occ WHERE occ.countrycode = 'CL'",
        "FROM cattest.occurrence occ\nWHERE occ.countrycode = 'CL'"),
      Arguments.of(
        "SELECT gbifid FROM occurrence AS occ WHERE occ.countrycode = 'CL'",
        "FROM cattest.occurrence occ\nWHERE occ.countrycode = 'CL'")
    );
  }

  private static Stream<Arguments> provideStringsForForbiddenSql() {
    return Stream.of(
        // Block subselect
        Arguments.of(
            "SELECT datasetkey, COUNT(*) FROM occurrence "
                + "WHERE countryCode IN (SELECT DISTINCT countryCode FROM occurrence WHERE \"month\" = 12)"),
        // Block query hints
        Arguments.of("SELECT /*+ MAPJOIN(time_dim) */ datasetkey FROM occurrence"),
        // Block joins
        Arguments.of(
            "SELECT o1.datasetkey, COUNT(DISTINCT o1.\"month\") FROM occurrence o1 INNER JOIN occurrence o2 ON o1.datasetkey = o2.datasetkey "
                + "GROUP BY o1.datasetkey"),
        Arguments.of(
            "SELECT o1.datasetkey, COUNT(DISTINCT o1.\"month\") FROM occurrence o1, occurrence o2 "
                + "WHERE o1.datasetkey = o2.datasetkey "
                + "GROUP BY o1.datasetkey"),
        Arguments.of(
            "SELECT o1.datasetkey, COUNT(DISTINCT o1.\"month\") FROM occurrence o1, occurrence o2 "
                + "GROUP BY o1.datasetkey"),
        // Block * select
        Arguments.of("SELECT o1.* FROM occurrence o1"),
        // Block with
        Arguments.of(
            "WITH subquery AS (SELECT DISTINCT datasetkey FROM occurrence) SELECT sq.datasetkey FROM subquery sq"),
        Arguments.of(
            "WITH subquery AS (SELECT DISTINCT datasetkey FROM occurrence) SELECT sq.datasetkey FROM subquery sq, occurrence o"),
        // Block modification of data
        Arguments.of("DELETE FROM occurrence"),
        Arguments.of(
            "INSERT INTO occurrence (gbifid, datasetkey, countryCode, \"day\") VALUES (1234, 'aoeuaoeuaoeu', 'XX', 0)"),
        Arguments.of("UPDATE occurrence SET datasetkey = 'AOEU'"),
        Arguments.of("TRUNCATE occurrence"),
        Arguments.of("DROP TABLE occurrence"),

        // Block having, due to citation difficulty.
        Arguments.of(
            "SELECT datasetkey, COUNT(*) FROM occurrence "
                + "GROUP BY datasetkey, countrycode "
                + "HAVING COUNT(*) > 2"),
        // Block qualify for the same reason
        Arguments.of(
            "SELECT datasetkey, COUNT(DISTINCT \"day\"), SUM(COUNT(*)) OVER (PARTITION BY \"month\" ORDER BY datasetkey) AS month_count "
                + "FROM occurrence "
                + "GROUP BY datasetkey, \"month\" "
                + "QUALIFY month_count > 100"),

        // SQL array constructor, not supported by Hive
        Arguments.of(
            "SELECT DISTINCT datasetkey FROM occurrence WHERE issue = array['ZERO_COORDINATE']"),

        // Block incorrect table and column names, commands and syntax
        Arguments.of("SELECT datasetkey FROM wrongTable occurrence"),
        Arguments.of("SELECT wrongColumn FROM occurrence"),
        Arguments.of("WRONGCOMMAND datasetkey FROM occurrence"),
        Arguments.of("FROM occurrence SELECT datasetkey"),
        Arguments.of("SELECT datasetkey FROM occurrence; SELECT datasetkey FROM occurrence"));

    // TODO: Block GROUP BY gbifid
    // Arguments.of("SELECT gbifid FROM occurrence GROUP BY gbifid"),
    // TODO: Block ORDER BY gbifid
    // Arguments.of("SELECT gbifid FROM occurrence ORDER BY gbifid"),
  }

  private static Stream<Arguments> provideStringsForBlockedHiveFunctions() {
    return Stream.of(
        // Logical operators — block EXISTS(subquery)
        Arguments.of(
            "SELECT EXISTS(SELECT datasetkey FROM occurrence) FROM occurrence;",
            "exactly one SQL select statement"),

        // Misc functions
        Arguments.of(
            "SELECT JAVA_METHOD('org.gbif', 'X') FROM occurrence",
            "No match found for function signature java_method"),
        Arguments.of(
            "SELECT REFLECT('org.gbif', 'X')  FROM occurrence",
            "No match found for function signature reflect"),

        // Arguments.of("SELECT CURRENT_USER FROM occurrence", "No match found for function
        // signature current_user"),

        Arguments.of(
            "SELECT LOGGED_IN_USER() FROM occurrence",
            "No match found for function signature logged_in_user"),
        Arguments.of(
            "SELECT CURRENT_DATABASE() FROM occurrence",
            "No match found for function signature current_database"),
        Arguments.of(
            "SELECT VERSION() FROM occurrence", "No match found for function signature version"),
        Arguments.of(
            "SELECT BUILDVERSION() FROM occurrence",
            "No match found for function signature buildversion"),

        // Table-Generating Functions (UDTF)
        Arguments.of(
            "SELECT SURROGATE_KEY() FROM occurrence",
            "No match found for function signature surrogate_key"),
        Arguments.of(
            "SELECT EXPLODE(issue) FROM occurrence",
            "No match found for function signature explode"),

        // Arguments.of("SELECT EXPLODE(map_type?) FROM occurrence", "No match found for function
        // signature explode"),

        Arguments.of(
            "SELECT POSEXPLODE(issue) FROM occurrence",
            "No match found for function signature posexplode"),
        Arguments.of(
            "SELECT INLINE(issue) FROM occurrence", "No match found for function signature inline"),
        Arguments.of(
            "SELECT STACK(2, 'a', 'b') FROM occurrence",
            "No match found for function signature stack"),
        Arguments.of(
            "SELECT JSON_TUPLE('{}', 'a') FROM occurrence",
            "No match found for function signature json_tuple"),
        Arguments.of(
            "SELECT PARSE_URL_TUPLE('x', 'HOST') FROM occurrence",
            "No match found for function signature parse_url_tuple"));
  }

  private static Stream<Arguments> provideStringsForBadSql() {
    return Stream.of(
        // Missing column
        Arguments.of(
            "SELECT gbifid FROM occurrence GROUP BY missing_column",
            "Column 'missing_column' not found in any table"),
        Arguments.of("SELECT country FROM occurrence", "Column 'country' not found in any table"),
        Arguments.of("SELECT year, gbifid FROM occurrence", "Encountered"),
        Arguments.of("SELECT \"YEAR\" FROM occurrence", "Column 'YEAR' not found in any table"),
        Arguments.of("SELECT gbifid FROM occurrence WHERE year > 10", "Encountered \"year >\""),
        Arguments.of(
            "SELECT gbifid FROM occurrence WHERE 'year' > 10",
            "string literals used in a comparison"),

        // Invalid grouping
        Arguments.of(
            "SELECT gbifid FROM occurrence GROUP BY datasetkey",
            "Expression 'gbifid' is not being grouped"),
        Arguments.of(
            "SELECT DISTINCT datasetkey, COUNT(*) FROM occurrence GROUP BY datasetkey",
            "SQL DISTINCT clauses cannot be combined with GROUP BY."),

        // Unknown function
        Arguments.of(
            "SELECT unknown_function(gbifid) FROM occurrence;",
            "No match found for function signature"),

        // Invalid polygon in GBIF_Within(...)
        Arguments.of(
          "SELECT gbifid FROM occurrence WHERE gbif_within('POLYGON ((30 10, 10 20, 20 40, 40 40))', decimalLatitude, decimalLongitude)",
          "Polygon used in GBIF_Within is invalid: Points of LinearRing do not form a closed linestring"),
        Arguments.of("SELECT scientificname, COUNT(*) FROM occurrence " +
            "WHERE GBIF_WITHIN('POLYGON ((-85.12207 22.390714, -74.311523 22.836946, -78.046875 15.623037, -84.858398 22.268764))', occurrence.decimallatitude, occurrence.decimallongitude) = TRUE " +
            "GROUP BY occurrence.scientificname", "Polygon used in GBIF_Within is invalid: Points of LinearRing do not form a closed linestring"),

        // Incorrect syntax
        Arguments.of("SELECT gbifid, FROM occurrence", "Incorrect syntax near the keyword"),
        Arguments.of("SELECT gbifid FROM occurrence,", "Encountered"),
        Arguments.of(
            "SELECT gbifid FROM occurrence GROUP BY gbifid, ORDER BY gbifid", "Encountered"),
        Arguments.of("SELECT gbifid FROM occurrence ORDER BY gbifid,", "Encountered"),
        Arguments.of("SELECT COALESCE(gbifid, 0 AS eeaCellCode FROM occurrence", "Encountered"),

        // Comments
        Arguments.of("SELECT -- Comment gbifid FROM occurrence", "Encountered"),

        // Unsupported syntax (see class for detailed reason)
        Arguments.of("SELECT gbifid FROM occurrence WHERE hascoordinate IS TRUE", "not supported"),
        Arguments.of(
            "SELECT gbifid FROM occurrence WHERE \"year\" BETWEEN 1980 AND 1990", "not supported"));
  }
}
