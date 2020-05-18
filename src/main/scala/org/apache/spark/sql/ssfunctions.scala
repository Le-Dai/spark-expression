package org.apache.spark.sql

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expression.{MethodInvokeExpress}

@InterfaceStability.Stable
object ssfunctions {
  private def withExpr(expr: Expression): Column = Column(expr)

  def codeInvoke(code: String,method: String,args: Expression*): Column = {
    val expressions = Seq(functions.lit(code).expr, functions.lit(method).expr).toBuffer
    expressions ++= args.toSeq
    org.apache.spark.sql.Column(MethodInvokeExpress(expressions))
  }
}
