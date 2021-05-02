package org.jetbrains.exposed.contrib.postgresql

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals

class ListColumnTest {
    @get:Rule
    val pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

    @Before
    fun createTable() {
        pg.embeddedPostgres.postgresDatabase.connection.use { connection ->
            connection.createStatement().use {
                it.execute("CREATE TABLE array_test (id SERIAL PRIMARY KEY, values varchar(20)[] NOT NULL)")
            }
        }
        Database.connect(pg.embeddedPostgres.postgresDatabase)
    }

    internal object ArrayTest : LongIdTable("array_test") {
        val values = list<String>("values", VarCharColumnType(20))
    }

    @Test
    fun testInsertAndGet() {
        transaction {
            val recordId = ArrayTest.insertAndGetId {
                it[values] = listOf("Foo", "Bar")
            }
            val loaded = ArrayTest.select { ArrayTest.id eq recordId }.first()
            val values = loaded[ArrayTest.values]
            assertEquals(2, values.size)
            assertEquals("Foo", values[0])
            assertEquals("Bar", values[1])
        }
    }
}