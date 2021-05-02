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


class AnyOp(val expr1: Expression<*>, val expr2: Expression<*>) : Op<Boolean>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) {
        queryBuilder.append(expr2)
        queryBuilder.append(" = ANY (")
        queryBuilder.append(expr1)
        queryBuilder.append(")")
    }
}

// TODO: type declaration doesn't work as desired yet
// For example it is still possible to call `any` on non-collection columns
// and mismatching element types
infix fun <S, T : Collection<S>> ExpressionWithColumnType<T>.any(t: S): Op<Boolean> {
    if (t == null) {
        return IsNullOp(this)
    }
    return AnyOp(this, QueryParameter(t, columnType))
}

class ContainsOp(expr1: Expression<*>, expr2: Expression<*>) : ComparisonOp(expr1, expr2, "@>")

infix fun <S, T : Collection<S>> ExpressionWithColumnType<T>.contains(array: Iterable<S>): Op<Boolean> =
    ContainsOp(this, QueryParameter(array, columnType))
