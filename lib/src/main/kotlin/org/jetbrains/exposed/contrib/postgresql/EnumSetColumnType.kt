package org.jetbrains.exposed.contrib.postgresql

import org.jetbrains.exposed.sql.ColumnType
import kotlin.reflect.KClass

class EnumSetColumnType<T : Enum<T>>(
    private val enumClass: KClass<T>,
    private val elementTypeName: String
) :
    ColumnType() {
    override fun sqlType() = "$elementTypeName ARRAY"
    override fun valueToDB(value: Any?): Any? {
        return when (value) {
            is Array<*> -> {
                val jdbcConnection = currentSqlConnection()
                jdbcConnection.createArrayOf(elementTypeName, value)
            }
            is Set<*> -> {
                val jdbcConnection = currentSqlConnection()
                jdbcConnection.createArrayOf(elementTypeName, value.map { it.toString() }.toTypedArray())
            }
            else -> {
                super.valueToDB(value)
            }
        }
    }

    override fun valueFromDB(value: Any): Set<T> {
        return when (value) {
            is java.sql.Array -> (value.array as Array<String>).map { java.lang.Enum.valueOf(enumClass.java, it) }
                .toSet()
            is Set<*> -> value as Set<T>
            else -> {
                error("Array not supported for this database")
            }
        }
    }
}
