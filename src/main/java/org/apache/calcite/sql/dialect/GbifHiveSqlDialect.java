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
import java.util.List;
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
        SqlNode taxonIDs = call.operand(1);

        writer.print("EXISTS(");

        // If the checklist key is a simple char literal, print it directly to avoid
        // SqlWriter inserting an extra space before the closing bracket when
        // unparsing the node. Otherwise, fall back to unparse
        if (checklistKey instanceof SqlCharStringLiteral) {
            // the variable in the lambda needs to be recognised column
            // to pass Spark SQL validation
            // (even though that column isnt used)
            writer.print("classifications[" + checklistKey + "], x -> x IN ");
        } else {
            writer.print("classifications[");
            checklistKey.unparse(writer, 0, 0);
            writer.print("], x -> x IN ");
        }

        // If the taxon IDs are an ARRAY value constructor, unparse just the
        // contents (e.g. ('a','b')) rather than the full ARRAY('a','b') form
        // which Hive/Spark lambda expects when used with the "IN" operator.
        if (taxonIDs.getKind() == SqlKind.OTHER_FUNCTION && taxonIDs instanceof SqlCall) {
            List<SqlNode> elems = ((SqlCall) taxonIDs).getOperandList();
            writer.print("(");
            for (int i = 0; i < elems.size(); i++) {
                if (i > 0) {
                    writer.print(", ");
                }

                SqlNode sqlNode = elems.get(i);
                if (sqlNode instanceof SqlCharStringLiteral) {
                    writer.print(sqlNode.toString());
                } else {
                    elems.get(i).unparse(writer, 0, 0);
                }
            }
            writer.print(")");
        } else {
            taxonIDs.unparse(writer, 0, 0);
        }

        writer.print(")");
    }
}
