package org.gbif.occurrence.query.sql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class TaxonLookupFunction extends SqlFunction {

    public static final TaxonLookupFunction INSTANCE =
            new TaxonLookupFunction();

    private TaxonLookupFunction() {
        super(
                "TAXON_LOOKUP",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.BOOLEAN,
                null,
                OperandTypes.family(
                        SqlTypeFamily.STRING,
                        SqlTypeFamily.ARRAY
                ),
                SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }
}