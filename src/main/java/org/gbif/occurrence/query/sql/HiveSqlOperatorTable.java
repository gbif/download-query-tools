package org.gbif.occurrence.query.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlDatePartFunction;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_CONTAINS;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.ARRAY_SIZE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CHR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.CONCAT_FUNCTION;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.FROM_HEX;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.GREATEST;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.INSTR;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LEAST;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LENGTH;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LPAD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.LTRIM;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.MD5;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.NVL;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REGEXP_REPLACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REPEAT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.REVERSE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RPAD;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.RTRIM;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SHA1;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SORT_ARRAY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SOUNDEX;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPACE;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SPLIT;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.SUBSTR_BIG_QUERY;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TO_BASE64;
import static org.apache.calcite.sql.fun.SqlLibraryOperators.TRANSLATE3;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.SIGN;
import static org.apache.calcite.sql.type.OperandTypes.family;

public class HiveSqlOperatorTable {

  List<SqlOperator> additionalOperators = new ArrayList<>();

  private HiveSqlOperatorTable() {
    SqlOperatorTable opTab =
      SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
        EnumSet.of(SqlLibrary.HIVE));
    additionalOperators.addAll(opTab.getOperatorList());

    // Mathematical functions
    final SqlFunction LOG2 =
      SqlBasicFunction.create("LOG2",
        ReturnTypes.DOUBLE_NULLABLE,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC);
    additionalOperators.add(LOG2);

    final SqlFunction CONV =
      SqlBasicFunction.create("CONV",
        ReturnTypes.VARCHAR,
        OperandTypes.or(
          OperandTypes.family(SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
          OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)),
        SqlFunctionCategory.NUMERIC);
    additionalOperators.add(CONV);

    additionalOperators.add(((SqlBasicFunction)SIGN).withName("POSITIVE"));
    additionalOperators.add(((SqlBasicFunction)SIGN).withName("NEGATIVE"));
    additionalOperators.add(((SqlBasicFunction)SIGN).withName("FACTORIAL"));

    final SqlFunction E =
      SqlBasicFunction.create("E", ReturnTypes.DOUBLE, OperandTypes.NILADIC,
          SqlFunctionCategory.NUMERIC)
        .withSyntax(SqlSyntax.FUNCTION_ID);
    additionalOperators.add(E);

    final SqlBasicFunction SHIFTLEFT =
      SqlBasicFunction.create("SHIFTLEFT",
        ReturnTypes.INTEGER,
        OperandTypes.NUMERIC_NUMERIC,
        SqlFunctionCategory.NUMERIC);
    additionalOperators.add(SHIFTLEFT);
    additionalOperators.add(SHIFTLEFT.withName("SHIFTRIGHT"));
    additionalOperators.add(SHIFTLEFT.withName("SHIFTRIGHTUNSIGNED"));

    final SqlFunction HEX =
      SqlBasicFunction.create("HEX",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.or(OperandTypes.INTEGER, OperandTypes.STRING),
        SqlFunctionCategory.STRING);
    additionalOperators.add(HEX);

    additionalOperators.add(((SqlBasicFunction)FROM_HEX).withName("UNHEX"));
    additionalOperators.add(GREATEST);
    additionalOperators.add(LEAST);

    // Collection functions
    additionalOperators.add(((SqlBasicFunction)ARRAY_SIZE).withName("SIZE"));

    additionalOperators.add(ARRAY_CONTAINS);
    additionalOperators.add(SORT_ARRAY);

    // Date functions
    final SqlFunction FROM_UNIXTIME = SqlBasicFunction.create("FROM_UNIXTIME",
      ReturnTypes.VARCHAR_2000, OperandTypes.or(
        family(SqlTypeFamily.NUMERIC),
        family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING)),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(FROM_UNIXTIME);

    final SqlFunction UNIX_TIMESTAMP = SqlBasicFunction.create("UNIX_TIMESTAMP",
      ReturnTypes.INTEGER,
      OperandTypes.or(
        OperandTypes.family(),
        OperandTypes.family(SqlTypeFamily.STRING),
        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(UNIX_TIMESTAMP);

    final SqlFunction TO_DATE = SqlBasicFunction.create("TO_DATE",
      ReturnTypes.VARCHAR,
      OperandTypes.family(SqlTypeFamily.STRING),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(TO_DATE);

    final SqlDatePartFunction WEEKOFYEAR = new SqlDatePartFunction("WEEKOFYEAR", TimeUnit.WEEK);
    additionalOperators.add(WEEKOFYEAR);

    final SqlFunction DATEDIFF = SqlBasicFunction.create("DATEDIFF",
      ReturnTypes.INTEGER,
      OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.DATE),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(DATEDIFF);

    final SqlBasicFunction DATE_ADD = SqlBasicFunction.create("DATE_ADD",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.INTEGER),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(DATE_ADD);
    additionalOperators.add(DATE_ADD.withName("DATE_SUB"));

    final SqlBasicFunction FROM_UTC_TIMESTAMP = SqlBasicFunction.create("FROM_UTC_TIMESTAMP",
      ReturnTypes.TIMESTAMP,
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(FROM_UTC_TIMESTAMP);
    additionalOperators.add(FROM_UTC_TIMESTAMP.withName("TO_UTC_TIMESTAMP"));

    final SqlFunction ADD_MONTHS = SqlBasicFunction.create("ADD_MONTHS",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(ADD_MONTHS);

    final SqlFunction NEXT_DAY = SqlBasicFunction.create("NEXT_DAY",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.STRING_STRING,
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(NEXT_DAY);

    final SqlFunction TRUNC = SqlBasicFunction.create("TRUNC",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.DATE_CHARACTER,
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(TRUNC);

    final SqlFunction MONTHS_BETWEEN = SqlBasicFunction.create("MONTHS_BETWEEN",
      ReturnTypes.DOUBLE,
      OperandTypes.family(SqlTypeFamily.DATE, SqlTypeFamily.DATE),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(MONTHS_BETWEEN);

    final SqlFunction DATE_FORMAT = SqlBasicFunction.create("DATE_FORMAT",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING),
      SqlFunctionCategory.TIMEDATE);
    additionalOperators.add(DATE_FORMAT);

    // Conditional functions
    final SqlBasicFunction ISNULL = SqlBasicFunction.create("ISNULL", ReturnTypes.BOOLEAN, OperandTypes.ANY);
    additionalOperators.add(ISNULL);
    additionalOperators.add(ISNULL.withName("ISNOTNULL"));

    additionalOperators.add(NVL);

    final SqlFunction ASSERT_TRUE = SqlBasicFunction.create("ASSERT_TRUE", ReturnTypes.BOOLEAN, OperandTypes.BOOLEAN);
    additionalOperators.add(ASSERT_TRUE);

    // String functions
    additionalOperators.add(((SqlBasicFunction)TO_BASE64).withName("BASE64"));
    additionalOperators.add(CHR);
    additionalOperators.add(CONCAT_FUNCTION);

    final SqlFunction CONCAT_WS =
      SqlBasicFunction.create("CONCAT_WS",
          ReturnTypes.MULTIVALENT_STRING_WITH_SEP_SUM_PRECISION_ARG0_NULLABLE,
          OperandTypes.or(OperandTypes.repeat(SqlOperandCountRanges.from(2), OperandTypes.STRING),
            OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.ARRAY)),
          SqlFunctionCategory.STRING)
        .withOperandTypeInference(InferTypes.RETURN_TYPE);
    additionalOperators.add(CONCAT_WS);

    final SqlFunction DECODE =
      SqlBasicFunction.create("DECODE",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.family(SqlTypeFamily.BINARY, SqlTypeFamily.STRING),
        SqlFunctionCategory.STRING);
    additionalOperators.add(DECODE);

    final SqlFunction ELT =
      SqlBasicFunction.create("ELT",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.STRING);
    additionalOperators.add(ELT);

    final SqlFunction ENCODE =
      SqlBasicFunction.create("ENCODE",
        ReturnTypes.VARBINARY_NULLABLE,
        OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING),
        SqlFunctionCategory.STRING);
    additionalOperators.add(ENCODE);

    final SqlFunction FIELD =
      SqlBasicFunction.create("FIELD",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.STRING);
    additionalOperators.add(FIELD);

    final SqlFunction FIND_IN_SET =
      SqlBasicFunction.create("FIND_IN_SET",
        ReturnTypes.INTEGER_NULLABLE,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(FIND_IN_SET);

    final SqlFunction FORMAT_NUMBER =
      SqlBasicFunction.create("FORMAT_NUMBER",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.or(
          OperandTypes.NUMERIC_NUMERIC,
          family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)),
        SqlFunctionCategory.STRING);
    additionalOperators.add(FORMAT_NUMBER);

    final SqlFunction GET_JSON_OBJECT =
      SqlBasicFunction.create("GET_JSON_OBJECT",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(GET_JSON_OBJECT);

    final SqlFunction IN_FILE =
      SqlBasicFunction.create("IN_FILE",
        ReturnTypes.BOOLEAN,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(IN_FILE);

    additionalOperators.add(INSTR);
    additionalOperators.add(LENGTH);

    final SqlFunction LOCATE =
      SqlBasicFunction.create("LOCATE",
        ReturnTypes.INTEGER,
        family(ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), (number) -> number == 2),
        SqlFunctionCategory.STRING);
    additionalOperators.add(LOCATE);

    additionalOperators.add(LPAD);
    additionalOperators.add(LTRIM);

    final SqlFunction PARSE_URL =
      SqlBasicFunction.create("PARSE_URL",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.STRING_STRING_OPTIONAL_STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(PARSE_URL);

    final SqlFunction PRINTF =
      SqlBasicFunction.create("PRINTF",
        ReturnTypes.VARCHAR,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.STRING);
    additionalOperators.add(PRINTF);

    final SqlFunction QUOTE =
      SqlBasicFunction.create("QUOTE",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(QUOTE);

    final SqlBasicFunction REGEXP_EXTRACT =
      SqlBasicFunction.create("REGEXP_EXTRACT",
        ReturnTypes.VARCHAR_NULLABLE,
        family(ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER), i -> i == 2 || i == 3),
        SqlFunctionCategory.STRING);
    additionalOperators.add(REGEXP_EXTRACT);

    additionalOperators.add(REGEXP_REPLACE);
    additionalOperators.add(REPEAT);
    additionalOperators.add(REVERSE);
    additionalOperators.add(RPAD);
    additionalOperators.add(RTRIM);
    additionalOperators.add(SPACE);
    additionalOperators.add(SPLIT);
    additionalOperators.add(SUBSTR_BIG_QUERY);

    final SqlBasicFunction SUBSTRING_INDEX =
      SqlBasicFunction.create("SUBSTRING_INDEX",
        ReturnTypes.VARCHAR_NULLABLE,
        OperandTypes.STRING_STRING_INTEGER,
        SqlFunctionCategory.STRING);
    additionalOperators.add(SUBSTRING_INDEX);

    additionalOperators.add(TRANSLATE3);
    additionalOperators.add(((SqlBasicFunction)FROM_BASE64).withName("UNBASE64"));
    final SqlFunction LEVENSHTEIN =
      SqlBasicFunction.create("LEVENSHTEIN",
        ReturnTypes.INTEGER_NULLABLE,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(LEVENSHTEIN);

    additionalOperators.add(SOUNDEX);

    // Data masking functions
    final SqlFunction MASK = SqlBasicFunction.create("MASK",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.repeat(SqlOperandCountRanges.between(1, 4), OperandTypes.STRING),
      SqlFunctionCategory.STRING);
    additionalOperators.add(MASK);

    final SqlBasicFunction MASK_FIRST_N = SqlBasicFunction.create("MASK_FIRST_N",
      ReturnTypes.VARCHAR_NULLABLE,
      family(ImmutableList.of(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER), (number) -> number == 1),
      SqlFunctionCategory.STRING);
    additionalOperators.add(MASK_FIRST_N);
    additionalOperators.add(MASK_FIRST_N.withName("MASK_LAST_N"));
    additionalOperators.add(MASK_FIRST_N.withName("MASK_SHOW_FIRST_N"));
    additionalOperators.add(MASK_FIRST_N.withName("MASK_SHOW_LAST_N"));

    final SqlFunction MASK_HASH = SqlBasicFunction.create("MASK_HASH",
      ReturnTypes.VARCHAR_NULLABLE,
      OperandTypes.ANY,
      SqlFunctionCategory.STRING);
    additionalOperators.add(MASK_HASH);

    // Misc functions
    final SqlFunction HASH =
      SqlBasicFunction.create("HASH",
        ReturnTypes.INTEGER,
        OperandTypes.repeat(SqlOperandCountRanges.from(1), OperandTypes.ANY),
        SqlFunctionCategory.STRING);
    additionalOperators.add(HASH);

    additionalOperators.add(MD5);
    additionalOperators.add(SHA1);
    additionalOperators.add(((SqlBasicFunction)SHA1).withName("SHA2"));
    additionalOperators.add(((SqlBasicFunction)SHA1).withName("CRC32"));

    final SqlBasicFunction AES_ENCRYPT =
      SqlBasicFunction.create("AES_ENCRYPT",
        ReturnTypes.VARBINARY_NULLABLE,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING);
    additionalOperators.add(AES_ENCRYPT);
    additionalOperators.add(AES_ENCRYPT.withName("AES_DECRYPT"));

/*    // Aggregate functions

    // Table-generating functions

    // Utility functions
*/
  }

  public List<SqlOperator> getAdditionalOperators() {
    return additionalOperators;
  }

  private static @MonotonicNonNull HiveSqlOperatorTable instance;

  public static synchronized HiveSqlOperatorTable instance() {
    if (instance == null) {
      instance = new HiveSqlOperatorTable();
    }
    return instance;
  }
}
