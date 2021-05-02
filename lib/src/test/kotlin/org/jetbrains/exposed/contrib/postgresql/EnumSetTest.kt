package org.jetbrains.exposed.contrib.postgresql

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule
import org.jetbrains.exposed.dao.id.LongIdTable
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import kotlin.test.assertEquals

class EnumSetTest {
    @get:Rule
    val pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

    @Before
    fun createEnumAndTable() {
        pg.embeddedPostgres.postgresDatabase.connection.use { connection ->
            connection.createStatement().use {
                it.execute("CREATE TYPE foo AS ENUM('bar', 'baz')")
                it.execute("CREATE TABLE foo_test (id SERIAL PRIMARY KEY, values foo[] NOT NULL)")
            }
        }
        Database.connect(pg.embeddedPostgres.postgresDatabase)
    }

    internal enum class Foo {
        bar,
        baz;
    }

    internal object FooTest : LongIdTable("foo_test") {
        val values = enumSet("values", Foo::class, "foo")
    }

    @Test
    fun testMappingViaContains() {
        transaction {
            FooTest.insertAndGetId {
                it[values] = setOf(Foo.bar, Foo.baz)
            }
            FooTest.insertAndGetId {
                it[values] = setOf(Foo.bar)
            }
            FooTest.insert {
                it[values] = setOf(Foo.baz)
            }
            val queryForBar = FooTest.select { FooTest.values contains setOf(Foo.bar) }.count()
            assertEquals(2, queryForBar)
            val queryForBoth = FooTest.select { FooTest.values contains setOf(Foo.bar, Foo.baz) }.count()
            assertEquals(1, queryForBoth)
        }
    }
}