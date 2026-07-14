package org.gbif.occurrence.query.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class LambdaUtil {

    private static final Pattern LAMBDA_PATTERN = Pattern.compile(
            "LAMBDA\\s*\\(\\s*([^,]+?)\\s*,\\s*(.*?)\\s*\\)",
            Pattern.CASE_INSENSITIVE);

    public static String convertLambda(String sql) {
        Matcher matcher = LAMBDA_PATTERN.matcher(sql);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String variable = matcher.group(1).trim();
            String expression = matcher.group(2).trim();

            // Replace all occurrences of the variable with x
            expression = expression.replaceAll(
                    "\\b" + Pattern.quote(variable) + "\\b",
                    "x");

            matcher.appendReplacement(result,
                    Matcher.quoteReplacement("x -> " + expression));
        }

        matcher.appendTail(result);
        return result.toString();
    }

    /**
     * Replace occurrences of `<identifier> -> <expression>` with `LAMBDA(<identifier>, <expression>)`.
     * This is a simple, local parser that handles nested parentheses in the expression.
     */
    public static String transformLambdaSyntax(String sql) {
        StringBuilder sb = new StringBuilder(sql);
        int idx = 0;
        while ((idx = sb.indexOf("->", idx)) != -1) {
            // find identifier to the left
            int left = idx - 1;
            // skip whitespace leftwards
            while (left >= 0 && Character.isWhitespace(sb.charAt(left))) {
                left--;
            }
            int endIdent = left;
            // find start of identifier
            while (left >= 0 && (Character.isLetterOrDigit(sb.charAt(left)) || sb.charAt(left) == '_')) {
                left--;
            }
            int startIdent = left + 1;
            if (startIdent > endIdent) {
                // no valid identifier found, skip this occurrence
                idx += 2;
                continue;
            }
            String ident = sb.substring(startIdent, endIdent + 1);

            // find start of expression to the right of ->
            int right = idx + 2;
            while (right < sb.length() && Character.isWhitespace(sb.charAt(right))) {
                right++;
            }

            // parse until a comma or closing parenthesis at depth 0
            int depth = 0;
            int exprEnd = right;
            while (exprEnd < sb.length()) {
                char c = sb.charAt(exprEnd);
                if (c == '(') {
                    depth++;
                } else if (c == ')') {
                    if (depth == 0) {
                        break;
                    }
                    depth--;
                } else if (c == ',' && depth == 0) {
                    break;
                }
                exprEnd++;
            }

            String expr = sb.substring(right, exprEnd).trim();

            String replacement = "LAMBDA(" + ident + ", " + expr + ")";
            sb.replace(startIdent, exprEnd, replacement);

            // continue scanning after the replacement
            idx = startIdent + replacement.length();
        }
        return sb.toString();
    }
}
