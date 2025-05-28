package org.apache.calcite.sql.dialect;

public class GbifHiveSqlDialect extends HiveSqlDialect {

  public GbifHiveSqlDialect(Context context) {
    super(context);
  }

  // No need to quote or escape Unicode strings.
  // https://issues.apache.org/jira/browse/CALCITE-3933
  // https://issues.apache.org/jira/browse/CALCITE-6001
  @Override
  public void quoteStringLiteralUnicode(StringBuilder buf, String val) {
      buf.append(literalQuoteString);
      buf.append(val.replace(literalEndQuoteString, literalEscapedQuote));
      buf.append(literalEndQuoteString);
  }
}
