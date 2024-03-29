/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.calcite.sql.dialect;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.RelToSqlConverterUtil;
import com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.apache.calcite.sql.fun.SqlBetweenOperator.*;

/**
 * A SqlDialect implementation for the Firebolt database.
 */
public class FireboltSqlDialect extends SqlDialect {
    public static final Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
            //.withDatabaseProduct("FIREBOLT") //DatabaseProduct.FIREBOLT
            .withIdentifierQuoteString("\"")
            .withNullCollation(NullCollation.LOW);
    public static final SqlDialect DEFAULT =
            new FireboltSqlDialect(DEFAULT_CONTEXT);
    /** Creates a FireboltSqlDialect. */
    public FireboltSqlDialect(Context context) {
        super(context);
    }

    /** Creates a FireboltSqlDialect. */
    public FireboltSqlDialect() {
        super(DEFAULT_CONTEXT);
    }
    /** Reserved Keywords for Firebolt. */
    private static final List<String> RESERVED_KEYWORDS =
            ImmutableList.copyOf(
                    Arrays.asList("ALL", "ALTER", "AND", "ARRAY", "BETWEEN",
                            "BIGINT", "BOOL", "BOOLEAN", "BOTH", "CASE",
                            "CAST", "CHAR", "CONCAT", "COPY", "CREATE", "CROSS",
                            "CURRENT_DATE", "CURRENT_TIMESTAMP", "DATABASE",
                            "DATE", "DATETIME", "DECIMAL", "DELETE", "DESCRIBE",
                            "DISTINCT", "DOUBLE", "DOUBLECOLON", "DOW", "DOY",
                            "DROP", "EMPTY_IDENTIFIER", "EPOCH", "EXCEPT", "EXECUTE",
                            "EXISTS", "EXPLAIN", "EXTRACT", "FALSE",
                            "FETCH", "FIRST", "FLOAT", "FROM", "FULL", "GENERATE",
                            "GROUP", "HAVING", "IF", "ILIKE", "IN", "INNER",
                            "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "IS",
                            "ISNULL", "JOIN", "JOIN_TYPE", "LEADING",
                            "LEFT", "LIKE", "LIMIT", "LIMIT_DISTINCT", "LOCALTIMESTAMP",
                            "LONG", "NATURAL", "NEXT", "NOT", "NULL",
                            "NUMERIC", "OFFSET", "ON", "ONLY", "OR", "ORDER", "OUTER",
                            "OVER", "PARTITION", "PRECISION", "PREPARE",
                            "PRIMARY", "QUARTER", "RIGHT", "ROW", "ROWS", "SAMPLE",
                            "SELECT", "SET", "SHOW", "TEXT", "TIME", "TIMESTAMP",
                            "TOP", "TRAILING", "TRIM", "TRUE", "TRUNCATE", "UNION",
                            "UNKNOWN_CHAR", "UNNEST", "UNTERMINATED_STRING",
                            "UPDATE", "USING", "VARCHAR", "WEEK", "WHEN", "WHERE",
                            "WITH"));
    /** An unquoted Firebolt identifier must start with a letter and be followed
     * by zero or more letters, digits or _. */
    private static final Pattern IDENTIFIER_REGEX =
            Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

    static final SqlWriter.FrameType FRAME_TYPE =
            SqlWriter.FrameTypeEnum.create("BETWEEN");

    @Override public boolean supportsCharSet() {
        return false;
    }
    @Override public void unparseOffsetFetch(SqlWriter writer, @Nullable SqlNode offset,
                                             @Nullable SqlNode fetch) {
        unparseFetchUsingLimit(writer, offset, fetch);
    }
    @Override public boolean supportsAggregateFunction(SqlKind kind) {
        switch (kind) {
            case ANY_VALUE:
            case AVG:
            case COUNT:
            case MAX:
            case MIN:
            case STDDEV_SAMP:
            case SUM:
                return true;
            default:
                break;
        }
        return false;
    }

    @Override protected boolean identifierNeedsQuote(String val) {
        return IDENTIFIER_REGEX.matcher(val).matches()
                || RESERVED_KEYWORDS.contains(val.toUpperCase(Locale.ROOT));
    }
    @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
        String castSpec;
        switch (type.getSqlTypeName()) {
            case TINYINT:
            case SMALLINT:
                // Firebolt has no tinyint or smallint, so instead cast to INT
                // fall through
                castSpec = "INT";
                break;
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Firebolt has no TIME, TimeWithLocalTimezone and TimestampWithLocalTimezone
                // so instead cast all those to TIMESTAMP
                // fall through
                castSpec = "TIMESTAMP";
                break;
            case CHAR:
                // Firebolt has no CHAR, so instead cast to VARCHAR
                castSpec = "VARCHAR";
                break;
            case DECIMAL:
                // Firebolt has no DECIMAL, so instead cast to FLOAT
                castSpec = "FLOAT";
                break;
            case REAL:
                // Firebolt has no REAL, so instead cast to DOUBLE
                castSpec = "DOUBLE";
                break;
            default:
                return super.getCastSpec(type);
        }
        // returning a type specification representing a type.
        return new SqlDataTypeSpec(
                new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
                SqlParserPos.ZERO);
    }
    @Override public boolean supportsFunction(SqlOperator operator,
                                              RelDataType type, final List<RelDataType> paramTypes) {
        switch (operator.kind) {
            case LIKE:
                // introduces support for ILIKE as well
            case EXISTS:
                // introduces support for EXISTS as well
                return true;
            default:
                return super.supportsFunction(operator, type, paramTypes);
        }
    }
    @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        if (call.getOperator().toString().contains("ILIKE")) {
            call.operand(0).unparse(writer, 0, 0);
            if (isNegatedLike(call)) {
                writer.print("NOT ");
            }
            writer.print("ILIKE ");
            call.operand(1).unparse(writer, 0, 0);

            return;
        }
        if (call.getOperator() == SqlStdOperatorTable.SUBSTRING) {
            RelToSqlConverterUtil.specialOperatorByName("SUBSTR")
                    .unparse(writer, call, 0, 0);
        } else {
            switch (call.getKind()) {
                case FLOOR:
                    if (call.operandCount() != 2) {
                        super.unparseCall(writer, call, leftPrec, rightPrec);
                        return;
                    }
                    final SqlLiteral timeUnitNode = call.operand(1);
                    final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);
                    SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
                            timeUnitNode.getParserPosition());
                    SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
                    break;

                case BETWEEN:
                    SqlBetweenOperator sqlBetweenOperator = (SqlBetweenOperator) call.getOperator();
                    if (sqlBetweenOperator.flag == SqlBetweenOperator.Flag.ASYMMETRIC) {
                        final SqlWriter.Frame frame =
                                writer.startList(FRAME_TYPE, "", "");
                        call.operand(VALUE_OPERAND).unparse(writer, sqlBetweenOperator.getLeftPrec(), 0);
                        unparseBetweenOperatorRightSide(writer, call, sqlBetweenOperator, frame);
                    }
                    else {
                        super.unparseCall(writer, call, leftPrec, rightPrec);
                    }
                    break;

                default:
                    super.unparseCall(writer, call, leftPrec, rightPrec);
            }
        }
    }

    /** Firebolt interval syntax: sign INTERVAL int64 time_unit. */
    @Override public void unparseSqlIntervalLiteral(SqlWriter writer,
                                                    SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
        SqlIntervalLiteral.IntervalValue interval =
                literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
        writer.keyword("INTERVAL");
        writer.print("'");
        try {
            Long.parseLong(interval.getIntervalLiteral());
        } catch (NumberFormatException e) {
            throw new RuntimeException("Only INT64 is supported as the interval value for Firebolt.");
        }
        writer.literal(interval.getIntervalLiteral());
        unparseSqlIntervalQualifier(writer, interval.getIntervalQualifier(),
                RelDataTypeSystem.DEFAULT);
        writer.print("'");
    }

    @Override public void unparseSqlIntervalQualifier(
            SqlWriter writer, SqlIntervalQualifier qualifier, RelDataTypeSystem typeSystem) {
        final String start = validate(qualifier.timeUnitRange.startUnit).name();
        if (qualifier.timeUnitRange.endUnit == null) {
            writer.keyword(start);
        } else {
            throw new RuntimeException("Range time unit is not supported for Firebolt.");
        }
    }

    private static TimeUnit validate(TimeUnit timeUnit) {
        switch (timeUnit) {
            case MICROSECOND:
            case MILLISECOND:
            case SECOND:
            case MINUTE:
            case HOUR:
            case DAY:
            case WEEK:
            case MONTH:
            case YEAR:
            case DECADE:
            case CENTURY:
            case MILLENNIUM:
                return timeUnit;
            default:
                throw new RuntimeException("Time unit " + timeUnit + " is not supported for Firebolt.");
        }
    }

    private void unparseBetweenOperatorRightSide(SqlWriter writer, SqlCall call, SqlBetweenOperator sqlBetweenOperator,
                                                 SqlWriter.Frame frame) {
        writer.sep(sqlBetweenOperator.getName().contains("NOT BETWEEN") ? "NOT BETWEEN" : "BETWEEN");
        final SqlNode lower = call.operand(LOWER_OPERAND);
        final SqlNode upper = call.operand(UPPER_OPERAND);
        int lowerPrec = new SqlBetweenOperator.AndFinder().containsAnd(lower) ? 100 : 0;
        lower.unparse(writer, lowerPrec, lowerPrec);
        writer.sep("AND");
        upper.unparse(writer, 0, sqlBetweenOperator.getRightPrec());
        writer.endList(frame);
    }

    private boolean isNegatedLike(SqlCall call) {
        return (call.getOperator() instanceof SqlLikeOperator) && (((SqlLikeOperator)(call.getOperator())).isNegated());
    }
}