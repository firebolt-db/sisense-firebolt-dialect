/*
 * This is a custom sql dialect implementation for Firebolt database. 
 * It will be used by customers for creating live/elastic data models in Sisense and then further their respective dashboards.
 *
 * @Author: Raghav Sharma
 * @Date: 03/08/2022
 */

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.*;
import java.sql.Timestamp;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.sisense.translation.calcite.adapter.dialects.*;
import com.sisense.common.infra.Utils;

/**
 * A SqlDialect implementation for the Firebolt database.
 */
@Slf4j
public class FireboltSqlDialect extends SisenseDefaultDialect {

  /**
   * Creates a FireboltSqlDialect.
   *
   * @param databaseProduct       Database product; may be UNKNOWN, never null.
   * @param databaseProductName   Database product name from JDBC driver.
   * @param identifierQuoteString String to quote identifiers. Null if quoting
   *                              is not supported. If "[", close quote is
   *                              deemed to be "]".
   * @param unquotedCasing        upper or lower casing required for dialect.
   */
  public FireboltSqlDialect() {
    super(EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.POSTGRESQL)
            .withIdentifierQuoteString("\"")
            .withUnquotedCasing(Casing.TO_UPPER));
  }

  /**
   * Returns whether the dialect supports character set names as part of a
   * data type, for instance {VARCHAR(30) CHARACTER SET `ISO-8859-1`}.
   *
   * @return boolean
   */
  @Override public boolean supportsCharSet() {
    return false;
  }

  /**
   * Unparses the SQL call as per the kind and operators of SQL required for the database.
   *
   * @param writer       object of SqlWriter class.
   * @param call         object of SqlCall class; call to an SQL operator.
   * @param leftPrec     left precedence.
   * @param rightPrec    right precedence.
   */
  @Override public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
      final SqlLiteral timeUnitNode = call.operand(1);
      final TimeUnitRange timeUnit = timeUnitNode.getValueAs(TimeUnitRange.class);

      SqlCall call2 = SqlFloorFunction.replaceTimeUnitOperand(call, timeUnit.name(),
              timeUnitNode.getParserPosition());
      SqlFloorFunction.unparseDatetimeFunction(writer, call2, "DATE_TRUNC", false);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  /**
   * Returns SqlNode for type in "cast(column as type)", which might be
   * different for Firebolt by type name, precision etc.
   *
   * Firebolt datatypes reference: https://docs.firebolt.io/general-reference/data-types.html
   *
   * If this method returns null, the cast will be omitted. In the default
   * implementation, this is the case for the NULL type, and therefore
   * {CAST(NULL AS <nulltype>)} is rendered as {NULL}.
   *
   * @param type      object of RelDataType class; SQL datatype of column.
   * @return SqlNode  casts the datatype of column received into datatype supported by Firebolt.
   */
  @Override public @Nullable SqlNode getCastSpec(RelDataType type) {
    String castSpec;
    switch (type.getSqlTypeName()) {
      case TINYINT:
        // Firebolt has no tinyint, so instead cast to INT
        castSpec = "INT";
        break;
      case SMALLINT:
        // Firebolt has no smallint, so instead cast to INT
        castSpec = "INT";
        break;
      case TIME:
        // Firebolt has no TIME, so instead cast to TIMESTAMP
        castSpec = "TIMESTAMP";
        break;
      case TIME_WITH_LOCAL_TIME_ZONE:
        // Firebolt has no TimeWithTimezone, so instead cast to TIMESTAMP
        castSpec = "TIMESTAMP";
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // Firebolt has no TimestampWithTimezone, so instead cast to TIMESTAMP
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

    return new SqlDataTypeSpec(
            new SqlAlienSystemTypeNameSpec(castSpec, type.getSqlTypeName(), SqlParserPos.ZERO),
            SqlParserPos.ZERO);
  }

  /**
   * @// TODO:  Below methods require some class files that aren't accessible for now.
   * Once those are available, will check JDBC's behaviour. 
   *
  @Override
  public void unparseSqlDatetimeArithmetic(SqlWriter writer,
                                           SqlCall call, SqlKind sqlKind, int leftPrec, int rightPrec) {

    DatetimeArithmeticToDateAdd.unparseSqlDatetimeArithmetic(writer, call, sqlKind, leftPrec, rightPrec, "DATEADD");
  }

  @Override
  public void unparseSqlIntervalLiteral(
          SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {

    DatetimeArithmeticToDateAdd.unparseSqlIntervalLiteral(writer, literal, leftPrec, rightPrec);
  }

  public Boolean superUnparseOffsetFetch(OrderByInfoForNullsFirst orderByInfoForNullsFirst, SqlWriter writer) {
    super.unparseOffsetFetch(writer, orderByInfoForNullsFirst.getOffset(), orderByInfoForNullsFirst.getFetch());
    return true;
  }
  */
}
