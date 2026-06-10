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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.gbif.occurrence.query.sql.TaxonLookupFunction;

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
    @Override
    public void unparseCall(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {

        if (call.getOperator() == TaxonLookupFunction.INSTANCE) {
            unparseTaxonLookup(writer, call);
            return;
        }

        super.unparseCall(writer, call, leftPrec, rightPrec);
    }

    private void unparseTaxonLookup(SqlWriter writer, SqlCall call) {
        SqlNode checklistKey = call.operand(0);
        SqlNode taxonIds = call.operand(1);

        writer.print("EXISTS(");
        writer.print("classifications[");
        unparseNode(writer, checklistKey);
        writer.print("], x -> x IN ");

        unparseTaxonIds(writer, taxonIds);

        writer.print(")");
    }

    /**
     * This is necessary as node.unparse(writer, 0, 0) will add the keyword 'ARRAY'
     * which breaks spark sql queries. This is probably because we are extending the
     * HiveSqlDialect as opposed to the SparkSqlDialect.
     */
    private void unparseTaxonIds(SqlWriter writer, SqlNode taxonIds) {
        if (isArrayValueConstructor(taxonIds)) {
            SqlCall arrayCall = (SqlCall) taxonIds;

            writer.print("(");
            for (int i = 0; i < arrayCall.getOperandList().size(); i++) {
                if (i > 0) {
                    writer.print(", ");
                }

                unparseNode(writer, arrayCall.getOperandList().get(i));
            }
            writer.print(")");

            return;
        }

        taxonIds.unparse(writer, 0, 0);
    }

    /**
     * This tries to avoid superfluous spacing added after the literals
     */
    private void unparseNode(SqlWriter writer, SqlNode node) {
        if (node instanceof SqlCharStringLiteral) {
            writer.print(node.toString());
        } else {
            node.unparse(writer, 0, 0);
        }
    }

    private boolean isArrayValueConstructor(SqlNode node) {
        return node instanceof SqlCall
                && node.getKind() == SqlKind.OTHER_FUNCTION;
    }
}
