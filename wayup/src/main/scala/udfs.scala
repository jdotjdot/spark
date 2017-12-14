package org.apache.spark.sql.catalyst.expressions

object wayup {

  import java.util.Comparator

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
  import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
  import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.Column

  /**
    * Checks if both arrays have any values in common
    */
  //  @ExpressionDescription(
  //    usage = "_FUNC_(array, array) - Returns true if the arrays have any intersecting values.",
  //    examples = """
  //    Examples:
  //       > SELECT _FUNC_(array(1,2,3), array(2,5));
  //        true
  //     """
  //  )
  case class ArrayIntersect(left: Expression, right: Expression) extends
    BinaryExpression with ImplicitCastInputTypes {

    override def dataType: DataType = BooleanType

    override def inputTypes: Seq[AbstractDataType] = right.dataType match {
      case NullType => Seq.empty
      // We know both arrays are of the same type, so we can be a bit lazy here
      case _ => left.dataType match {
        case NullType => Seq.empty
        case n@ArrayType(element, _) => Seq(n, n)
        case _ => Seq.empty
      }
    }

    def checkIsArrayType(input: Expression): Boolean = {
      input.dataType.isInstanceOf[ArrayType]
    }

    override def checkInputDataTypes(): TypeCheckResult = {
      if (!checkIsArrayType(left) || !checkIsArrayType(right) ||
        left.dataType.asInstanceOf[ArrayType].elementType !=
          right.dataType.asInstanceOf[ArrayType].elementType) {
        TypeCheckResult.TypeCheckFailure(
          "Arguments must be two arrays with the same value types.")
      } else {
        TypeCheckResult.TypeCheckSuccess
      }
    }

    override def nullable: Boolean = {
      left.dataType.asInstanceOf[ArrayType].containsNull ||
        right.dataType.asInstanceOf[ArrayType].containsNull
    }

//    override def nullSafeEval(arr1: Any, arr2: Any): Any = {
//      var hasNull = false
//      arr1.asInstanceOf[ArrayData].foreach(left.dataType, (i, v) =>
//        if (v == null) {
//          hasNull = true
//        } else {
//          arr2.asInstanceOf[ArrayData].foreach(right.dataType, (ii, vv) =>
//            if (vv == null) {
//              hasNull = true
//            } else if (v == vv) {
//              return true
//            }
//          )
//        }
//      )
//      if (hasNull) {
//        null
//      } else {
//        false
//      }
//    }

    // TODO: get rid of null-handling since we want to _match_ nulls
    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      nullSafeCodeGen(ctx, ev, (arr1, arr2) => {
        val i = ctx.freshName("i")
        val j = ctx.freshName("j")
        val leftDataType = left.dataType.asInstanceOf[ArrayType].elementType
        val rightDataType = right.dataType.asInstanceOf[ArrayType].elementType
        val getValueLeft = ctx.getValue(arr1, leftDataType, i)
        val getValueRight = ctx.getValue(arr2, rightDataType, j)

//        mySet.retainAll(mySet2);
//        Set<T> mySet2 = new HashSet<T>(Arrays.asList($arr2));
//        s"""
//        java.util.Set<T> mySet = new java.util.HashSet<T>(Arrays.asList($arr1));
//
//        for (int $i = 0; $i < $arr2.numElements(); $i ++) {
//          if (mySet.contains(${ctx.getValue(arr2, leftDataType, i)})) {
//            ${ev.value} = true;
//            break;
//          }
//        }
//        ${ev.value} = false;
//         """

        s"""
      outerloop:
      for (int $i = 0; $i < $arr1.numElements(); $i ++) {
        if ($arr1.isNullAt($i)) {
          ${ev.isNull} = true;
        } else {
          for (int $j = 0; $j < $arr2.numElements(); $j ++) {
            if ($arr2.isNullAt($j)) {
              ${ev.isNull} = true;
            } else if (${ctx.genEqual(rightDataType, getValueLeft, getValueRight)}) {
              ${ev.isNull} = false;
              ${ev.value} = true;
              break outerloop;
            }
          }
        }
      }
       """
      })
    }

    override def prettyName: String = "array_intersect"
  }

  private def withExpr(expr: Expression): Column = Column(expr)

  def array_intersection_col(column: Column, column2: Column): Column = withExpr {
    ArrayIntersect(column.expr, column2.expr)
  }

  def array_intersection(leftColumnName: String, rightColumnName: String): Column = {
    array_intersection_col(Column(leftColumnName), Column(rightColumnName))
  }

}
