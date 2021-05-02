package org.jetbrains.exposed.contrib.postgresql

import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.jdbc.JdbcConnectionImpl
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.postgresql.util.PGobject
import kotlin.reflect.KClass

internal inline fun currentSqlConnection() = (TransactionManager.current().connection as JdbcConnectionImpl).connection

class PGEnum<T : Enum<T>>(enumTypeName: String, enumValue: T?) : PGobject() {
    init {
        value = enumValue?.name
        type = enumTypeName
    }
}

inline fun <reified T : Enum<T>> Table.pgEnum(sqlType: String, columnName: String): Column<T> {
    return customEnumeration(columnName, sqlType,
        { enumValueOf(it as String) },
        { PGEnum(sqlType, it) })
}

fun <T> Table.list(name: String, elementType: ColumnType): Column<List<T>> =
    registerColumn(name, ListColumnType<T>(elementType))

fun <T : Enum<T>> Table.enumSet(
    columnName: String,
    enumClass: KClass<T>,
    typeName: String
): Column<Set<T>> =
    registerColumn(columnName, EnumSetColumnType(enumClass, typeName))

//
//class AnyOp(val expr1: Expression<*>, val expr2: Expression<*>) : Op<Boolean>() {
//    private fun appendExpression(queryBuilder: QueryBuilder, expression: Expression<*>) {
//        when (expression) {
//            is OrOp -> queryBuilder.append("(").append(expression).append(")")
//            is QueryParameter -> {
//                val column = expression.value as Column<*>
//                queryBuilder.append("${column.table.tableName}.${column.name}")
//            }
//            else -> queryBuilder.append(expression)
//        }
//    }
//
//    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
//        appendExpression(queryBuilder, expr2)
//        queryBuilder.append(" = ANY (")
//        appendExpression(queryBuilder, expr1)
//        queryBuilder.append(")")
//    }
//}
//
//infix fun <T, S> ExpressionWithColumnType<T>.any(t: S): Op<Boolean> {
//    if (t == null) {
//        return IsNullOp(this)
//    }
//    return AnyOp(this, QueryParameter(t, columnType))
//}

class ContainsOp(expr1: Expression<*>, expr2: Expression<*>) : ComparisonOp(expr1, expr2, "@>")
infix fun <T, S> ExpressionWithColumnType<T>.contains(array: Iterable<S>): Op<Boolean> =
    ContainsOp(this, QueryParameter(array, columnType))
