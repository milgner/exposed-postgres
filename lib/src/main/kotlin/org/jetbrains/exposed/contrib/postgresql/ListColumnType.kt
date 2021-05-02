package org.jetbrains.exposed.contrib.postgresql

import org.jetbrains.exposed.sql.ColumnType

abstract class CollectionColumnType<T, S : Collection<T>> : ColumnType()

class ListColumnType<T>(private val elementType: ColumnType) : CollectionColumnType<T, List<T>>() {
    override fun sqlType(): String = "${elementType.sqlType()} ARRAY"

    override fun valueFromDB(value: Any): List<T> {
        val pureArray = when (value) {
            is java.sql.Array -> value.array
            is Array<*> -> value
            else -> {
                error("Database returned non-array value")
            }
        } as Array<Any>
        return pureArray.map { elementType.valueFromDB(it) as T }
    }

    override fun notNullValueToDB(value: Any): Any {
        return if (value is List<*>) {
            if (value.isEmpty())
                return "'{}'"

            val simpleType = elementType.sqlType().split("(")[0]
            val jdbcConnection = currentSqlConnection()
            jdbcConnection.createArrayOf(simpleType, value.toTypedArray())
                ?: error("Can't create array for array {${value.joinToString(",")}}")
        } else {
            super.notNullValueToDB(value)
        }
    }
}