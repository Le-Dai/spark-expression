package org.apache.spark.sql.expression

import java.lang.reflect.{Method, Modifier}

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.CallMethodViaReflection.typeMapping
import org.apache.spark.sql.catalyst.expressions.{CallMethodViaReflection, Expression, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenFallback}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal, StringType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.{ParentClassLoader, Utils}
import org.codehaus.janino.{ClassBodyEvaluator}

case class MethodInvokeExpress(children: Seq[Expression])
  extends Expression with CodegenFallback {

  override def prettyName: String = "code_invoke"

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.size < 2) {
      TypeCheckFailure("requires at least two arguments")
    } else if (!children.take(2).forall(e => e.dataType == StringType && e.foldable)) {
      // The first two arguments must be string type.
      TypeCheckFailure("first two arguments should be string literals")
    } else if (!classExists) {
      TypeCheckFailure(s"class $className not found")
    } else if (children.slice(2, children.length)
      .exists(e => !CallMethodViaReflection.typeMapping.contains(e.dataType))) {
      TypeCheckFailure("arguments from the third require boolean, byte, short, " +
        "integer, long, float, double or string expressions")
    } else if (method == null) {
      TypeCheckFailure(s"cannot find a method that matches the argument types in $className")
    } else {
      TypeCheckSuccess
    }
  }

  override lazy val deterministic: Boolean = false
  override def nullable: Boolean = true
  override val dataType: DataType = StringType

  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < argExprs.length) {
      buffer(i) = argExprs(i).eval(input).asInstanceOf[Object]
      // Convert if necessary. Based on the types defined in typeMapping, string is the only
      // type that needs conversion. If we support timestamps, dates, decimals, arrays, or maps
      // in the future, proper conversion needs to happen here too.
      if (buffer(i).isInstanceOf[UTF8String]) {
        buffer(i) = buffer(i).toString
      }
      i += 1
    }

    val ret = {
      if(isStatic){
        method.invoke(null, buffer : _*)
      }else {
        method.invoke(obj, buffer : _*)
      }
    }
    UTF8String.fromString(String.valueOf(ret))
  }

  @transient private lazy val argExprs: Array[Expression] = children.drop(2).toArray

  @transient private lazy val code = children(0).eval().asInstanceOf[UTF8String].toString

  /** Name of the class -- this has to be called after we verify children has at least two exprs. */
  @transient private lazy val className = "Eval"+children(1).eval().asInstanceOf[UTF8String].toString

  /** True if the class exists and can be loaded. */
  @transient private lazy val classExists = {
    if(isCompile && clazz !=null) true else false
  }

  @transient private lazy val isCompile = true

  @transient private lazy val obj = {
    MethodInvokeExpress.doCompile(code, className)
  }

  @transient private lazy val clazz = obj.getClass

  @transient lazy val method: Method = {
    val methodName = children(1).eval(null).asInstanceOf[UTF8String].toString
    val method = clazz.getMethods.find { method =>
      val candidateTypes = method.getParameterTypes
      if (method.getName != methodName) {
        // Name must match
        false
      }else if (candidateTypes.length != argExprs.map(_.dataType).length) {
        // Argument length must match
        false
      } else {
        // Argument type must match. That is, either the method's argument type matches one of the
        // acceptable types defined in typeMapping, or it is a super type of the acceptable types.
        candidateTypes.zip(argExprs.map(_.dataType)).forall { case (candidateType, argType) =>
          typeMapping(argType).exists(candidateType.isAssignableFrom)
        }
      }
    }.get
    if(Modifier.isStatic(method.getModifiers)) isStatic = true else isStatic = false
    method
  }

  /** A temporary buffer used to hold intermediate results returned by children. */
  @transient private lazy val buffer = new Array[Object](argExprs.length)

  @transient private var isStatic = false
}
object MethodInvokeExpress{
  /**
    * Returns true if the class can be found and loaded.
    */
  private def classExists(className: String): Boolean = {
    try {
      Utils.classForName(className)
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }

  /**
    * Compile the Java source code into a Java class, using Janino.
    */
  def doCompile(code: String, method: String) = {
    val evaluator = new ClassBodyEvaluator()

    val parentClassLoader = new ParentClassLoader(Utils.getContextOrSparkClassLoader)//Thread.currentThread().getContextClassLoader
    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName("com.scistor.expressions." + method)
    evaluator.setDefaultImports(Array(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName
    ))

    evaluator.setExtendedClass(classOf[Object])

    try {
      evaluator.cook(code)
    } catch {
      case e: Exception =>
        val msg = s"failed to compile: $e"
        throw new Exception(msg)
    }
    evaluator.getClazz.newInstance()
  }
}