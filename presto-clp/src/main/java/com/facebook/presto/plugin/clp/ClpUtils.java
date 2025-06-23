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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.log.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ClpUtils
{
    public static class KqlUtils
    {
        private static final Logger log = Logger.get(KqlUtils.class);

        public static String toSql(String kqlQuery)
        {
            String sqlQuery = kqlQuery;

            // Translate string filters
            Pattern stringFilterPattern = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*):\\s*\"([^\"]+)\"");
            Matcher stringFilterMatcher = stringFilterPattern.matcher(sqlQuery);
            StringBuilder translateStringFiltersSb = new StringBuilder();

            int lastStringFilterEnd = 0;
            while (stringFilterMatcher.find()) {
                String columnName = stringFilterMatcher.group(1);
                String value = stringFilterMatcher.group(2);

                // Append everything between last match and this one
                translateStringFiltersSb.append(sqlQuery, lastStringFilterEnd, stringFilterMatcher.start());

                if (value.contains("*")) {
                    translateStringFiltersSb.append("\"")
                            .append(columnName)
                            .append("\" LIKE '")
                            .append(value.replace('*', '%'))
                            .append("'");
                }
                else {
                    translateStringFiltersSb.append("\"")
                            .append(columnName)
                            .append("\" = '")
                            .append(value)
                            .append("'");
                }

                lastStringFilterEnd = stringFilterMatcher.end();
            }
            translateStringFiltersSb.append(sqlQuery.substring(lastStringFilterEnd));
            sqlQuery = translateStringFiltersSb.toString();

            // Translate numeric filters
            Pattern numericFilterPattern = Pattern.compile("([a-zA-Z_][a-zA-Z0-9_]*)\\s*([><]=?|:)\\s*(-?\\d+(?:\\.\\d+)?)");
            Matcher numericFilterMatcher = numericFilterPattern.matcher(sqlQuery);
            StringBuilder translateNumericFilterSb = new StringBuilder();

            int lastNumericFilterEnd = 0;
            while (numericFilterMatcher.find()) {
                String columnName = numericFilterMatcher.group(1);
                String operator = numericFilterMatcher.group(2);
                if (":".equals(operator)) {
                    operator = "=";
                }
                String value = numericFilterMatcher.group(3);

                // Append everything between last match and this one
                translateNumericFilterSb.append(sqlQuery, lastNumericFilterEnd, numericFilterMatcher.start());

                translateNumericFilterSb.append("\"")
                        .append(columnName)
                        .append("\" ")
                        .append(operator)
                        .append(" ")
                        .append(value);

                lastNumericFilterEnd = numericFilterMatcher.end();
            }
            translateNumericFilterSb.append(sqlQuery.substring(lastNumericFilterEnd));
            sqlQuery = translateNumericFilterSb.toString();

            return sqlQuery;
        }

        public static String escapeKqlSpecialCharsForColumnName(String columnName)
        {
            String escaped = columnName;
            escaped = escaped.replace("\\", "\\\\");
            escaped = escaped.replace("\"", "\\\"");
            escaped = escaped.replace(".", "\\.");
            escaped = escaped.replace("*", "\\*");
            if (escaped.startsWith("@")) {
                escaped = "\\@" + escaped.substring(1);
            }
            else if (escaped.startsWith("$")) {
                escaped = "\\$" + escaped.substring(1);
            }
            else if (escaped.startsWith("!")) {
                escaped = "\\!" + escaped.substring(1);
            }
            else if (escaped.startsWith("#")) {
                escaped = "\\#" + escaped.substring(1);
            }
            return escaped;
        }

        public static String escapeKqlSpecialCharsForStringValue(String literalString)
        {
            String escaped = literalString;
            escaped = escaped.replace("\\", "\\\\");
            escaped = escaped.replace("\"", "\\\"");
            escaped = escaped.replace("?", "\\?");
            escaped = escaped.replace("*", "\\*");
            return escaped;
        }
    }
}
