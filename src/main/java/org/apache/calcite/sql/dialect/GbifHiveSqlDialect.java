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
