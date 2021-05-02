package org.jetbrains.exposed.contrib.postgresql

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull


class SimpleEnumTest {
    @get:Rule
    val pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

    @Before
    fun createEnumAndTable() {
        pg.embeddedPostgres.postgresDatabase.connection.use { connection ->
            connection.createStatement().use {
                it.execute("CREATE TYPE foo AS ENUM('bar', 'baz')")
                it.execute("CREATE TABLE foo_test (id SERIAL PRIMARY KEY, value foo)")
            }
        }
        Database.connect(pg.embeddedPostgres.postgresDatabase)
    }

    internal enum class Foo {
        bar,
        baz;
    }

    internal object FooTest : LongIdTable("foo_test") {
        val value = pgEnum<Foo>("foo", "value").nullable()
    }

    @Test
    fun testEnumColumn() {
        transaction {
            val insertedIdBar = FooTest.insertAndGetId {
                it[value] = Foo.bar
            }
            val insertedIdBaz = FooTest.insertAndGetId {
                it[value] = Foo.baz
            }
            val retrievedBaz = FooTest.select { FooTest.value eq Foo.baz }.first()
            val retrievedBar = FooTest.select { FooTest.value eq Foo.bar }.first()
            assertEquals(insertedIdBar, retrievedBar[FooTest.id])
            assertEquals(insertedIdBaz, retrievedBaz[FooTest.id])
        }
    }

    @Test
    fun testWithNullValues() {
        transaction {
            val inserted = FooTest.insertAndGetId {
                // intentionally left blank
            }
            val retrieved = FooTest.select { FooTest.id eq inserted }.first()
            assertNull(retrieved[FooTest.value])
        }
    }
}