package org.jetbrains.exposed.contrib.postgresql

import kotlin.reflect.KClass

class EnumSetColumnType<T : Enum<T>>(
    private val enumClass: KClass<T>,
    private val elementTypeName: String
) :
    CollectionColumnType<T, Set<T>>() {
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
            // this is invoked when using the `any` predicate
            // sadly exact type checks aren't available but the Kotlin
            // type checks should prevent any unexpected surprises here
            is Enum<*> -> {
                PGEnum(elementTypeName, value as T)
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
