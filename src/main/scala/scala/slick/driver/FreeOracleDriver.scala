package scala.slick.driver

import scala.slick.ast._
import scala.slick.jdbc.JdbcType
import scala.slick.util.MacroSupport.macroSupportInterpolation
import scala.slick.profile.{ SqlProfile, Capability }
import scala.slick.compiler.{ Phase, QueryCompiler, CompilerState }

/**
 * Slick driver for Oracle..
 *
 * This driver implements the `scala.slick.driver.JdbcProfile`
 * ''without'' the following capabilities:
 *
 * <ul>
 *   <li>`scala.slick.driver.JdbcProfile.capabilities.returnInsertOther`:
 *     When returning columns from an INSERT operation, only a single column
 *     may be specified which must be the table's AutoInc column.</li>
 *   <li>`scala.slick.profile.SqlProfile.capabilities.sequence`:
 *     Sequences are not supported because SQLServer does not have this
 *     feature.</li>
 *   <li>`scala.slick.driver.JdbcProfile.capabilities.forceInsert`:
 *     Inserting explicit values into AutoInc columns with ''forceInsert''
 *     operations is not supported.</li>
 * </ul>
 */
trait FreeOracleDriver extends JdbcDriver { driver =>

  override protected def computeCapabilities: Set[Capability] = (super.computeCapabilities
    - JdbcProfile.capabilities.forceInsert
    - JdbcProfile.capabilities.returnInsertOther
    - SqlProfile.capabilities.sequence
  )

  override protected def computeQueryCompiler: QueryCompiler = (super.computeQueryCompiler
    + Phase.rewriteBooleans
  )

  override val columnTypes = new JdbcTypes
  override def createQueryBuilder(n: Node, state: CompilerState): QueryBuilder = new QueryBuilder(n, state)
  override def createColumnDDLBuilder(column: FieldSymbol, table: Table[_]): ColumnDDLBuilder = new ColumnDDLBuilder(column)

  override def defaultSqlTypeName(tmd: JdbcType[_]): String = tmd.sqlType match {
    case java.sql.Types.BOOLEAN => "CHAR"
    case _ => super.defaultSqlTypeName(tmd)
  }

  class QueryBuilder(tree: Node, state: CompilerState) extends super.QueryBuilder(tree, state) with OracleStyleRowNum {
    override protected val supportsTuples = false
    override protected val concatOperator = Some("+")

    override protected def buildOrdering(n: Node, o: Ordering) {
      if (o.nulls.last && !o.direction.desc)
        b"case when ($n) is null then 1 else 0 end,"
      else if (o.nulls.first && o.direction.desc)
        b"case when ($n) is null then 0 else 1 end,"
      expr(n)
      if (o.direction.desc) b" desc"
    }
  }

  class ColumnDDLBuilder(column: FieldSymbol) extends super.ColumnDDLBuilder(column) {
    override protected def appendOptions(sb: StringBuilder) {
      if (defaultLiteral ne null) sb append " DEFAULT " append defaultLiteral
      if (notNull) sb append " NOT NULL"
      if (primaryKey) sb append " PRIMARY KEY"
      if (autoIncrement) sb append " IDENTITY"
    }
  }

  class JdbcTypes extends super.JdbcTypes {
    override val booleanJdbcType = new BooleanJdbcType
    override val byteJdbcType = new ByteJdbcType
    override val dateJdbcType = new DateJdbcType
    override val timeJdbcType = new TimeJdbcType
    override val timestampJdbcType = new TimestampJdbcType
    override val uuidJdbcType = new UUIDJdbcType {
      override def sqlTypeName = "UNIQUEIDENTIFIER"
    }

    /* Oracle does not have a proper BOOLEAN type. The suggested workaround is
     * CHAR with constants 1 and 0 for TRUE and FALSE. */
    class BooleanJdbcType extends super.BooleanJdbcType {
      override def valueToSQLLiteral(value: Boolean) = if (value) "1" else "0"
    }
  }
}

object FreeOracleDriver extends FreeOracleDriver
