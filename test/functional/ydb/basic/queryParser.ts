import "reflect-metadata"

import { expect } from "chai"
import { DataSource } from "../../../../src/data-source/DataSource"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"
import { QueryRunner } from "../../../../src"

describe("ydb driver > queryParser", () => {
    let connection: DataSource
    let queryRunner: QueryRunner

    before(async () => {
        connection = (
            await createTestingConnections({
                entities: [],
                schemaCreate: true,
                dropSchema: true,
                enabledDrivers: ["ydb"],
            })
        )[0]
        queryRunner = connection.driver.createQueryRunner("master")
        await queryRunner.connect()
    })
    beforeEach(() => reloadTestingDatabases([connection]))
    after(() => closeTestingConnections([connection]))

    it("performs simple fields parsing", async () => {
        const query = `SELECT
        Bool("true"),
        Uint8("0"),
        Int32("-1"),
        Uint32("2"),
        Int64("-3"),
        Uint64("4"),
        Float("-5"),
        Double("6"),
        Decimal("1.23", 5, 2),
        String("foo"),
        Utf8("bar"),
        Yson("<a=1>[3;%false]"),
        Json(@@{"a":1,"b":null}@@),
        JsonDocument(@@{"a":1,"b":null}@@),
        Date("2017-11-27"),
        Datetime("2017-11-27T13:24:00Z"),
        Timestamp("2017-11-27T13:24:00.123456Z"),
        Interval("P1DT2H3M4.567890S"),
        TzDate("2017-11-27,Europe/Moscow"),
        TzDatetime("2017-11-27T13:24:00,America/Los_Angeles"),
        TzTimestamp("2017-11-27T13:24:00.123456,GMT"),
        Uuid("f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb"),
        DyNumber("1E-130")`
        const res = await queryRunner.query(query, [], true)
        expect(res).to.eq([1, "abc", 123.12])
    })

    it("performs containers parsing", async () => {
        const query = `SELECT
        AsList(1, 2, 3),
        AsDict(
          AsTuple("a", 1),
          AsTuple("b", 2),
          AsTuple("c", 3)
        ),
        AsSet(1, 2, 3),
        AsTuple(1, 2, "3"),
        AsStruct(
          1 AS a,
          2 AS b,
          "3" AS c
        )
        `
        const res = await queryRunner.query(query, [], true)
        expect(res).to.eq([1, "abc", 123.12])
    })

    it("performs enum, variant parsing", async () => {
        const resVariant = await queryRunner.query(
            `$var_type = Variant<foo: Int32, bar: Bool>;
        SELECT
           Variant(6, "foo", $var_type) as Variant1Value,
           Variant(false, "bar", $var_type) as Variant2Value;`,
            [],
            true,
        )
        expect(resVariant).to.eq([1, "abc", 123.12])

        const resEnum = await queryRunner.query(
            `$enum_type = Enum<Foo, Bar>;
        SELECT
           Enum("Foo", $enum_type) as Enum1Value,
           Enum("Bar", $enum_type) as Enum2Value;`,
            [],
            true,
        )
        expect(resEnum).to.eq([1, "abc", 123.12])
    })

    // not testing parsing of: - not serializing
    //  stream
    //  Callable
    //  Resource
    //  Generic
    //  Unit
    //  Void

    // not testing parsing of: - throwing YDB errors
    //  AsList(), -- EmptyList
    //  AsDict() -- EmptyDict

    it("performs special types parsing", async () => {
        const query = `SELECT AsTagged(1, "Foo"), -- Tagged
        FIND("abcdefg_abcdefg", "abc", 9), -- Null`
        const res = await queryRunner.query(query, [], true)
        expect(res).to.eq([1, "abc", 123.12])
    })

    it("performs cast types parsing", async () => {
        const query = `SELECT CAST(Null AS Void),
        CAST(1 AS Bool),
        CAST(-2 AS Int8),
        CAST(3 AS Uint8),
        CAST(-4 AS Int16),
        CAST(5 AS Uint16),
        CAST(-6 AS Int32),
        CAST(7 AS Uint32),
        CAST(-8 AS Int64),
        CAST(9 AS Uint64),
        CAST(10.10 AS Float),
        CAST(11.11 AS Double),
        CAST(12 AS Date),
        CAST(13 AS Datetime),
        CAST(14 AS Timestamp),
        CAST(15 AS Interval),
        CAST(16 AS TzDate),
        CAST(17 AS TzDatetime),
        CAST(18 AS TzTimestamp),
        CAST(0xA19F AS Bytes),
        CAST(0x20 AS Text),
        CAST('<a=z;x=y>[{abc=123; def=456};{abc=234; xyz=789};]' AS YSON),
        CAST('{"firstName": "John","lastName": "Smith","isAlive": true,"age": 27,"address": {"streetAddress": "21 2nd Street","postalCode": "10021-3100"},"phoneNumbers": [{"type": "office","number": "646 555-4567"}],"children": ["Catherine","Trevor"],"spouse": null}' AS JSON),
        CAST('34ba4833-d48f-5655-8113-e247da8fe502' AS UUID),
        CAST('{"firstName": "John","lastName": "Smith","isAlive": true,"age": 27,"address": {"streetAddress": "21 2nd Street","postalCode": "10021-3100"},"phoneNumbers": [{"type": "office","number": "646 555-4567"}],"children": ["Catherine","Trevor"],"spouse": null}' AS JSONDocument),
        CAST("1E-130" AS DyNumber),
        -- optional fields
        CAST(Null AS Void?),
        CAST(1 AS Bool?),
        CAST(-2 AS Int8?),
        CAST(3 AS Uint8?),
        CAST(-4 AS Int16?),
        CAST(5 AS Uint16?),
        CAST(-6 AS Int32?),
        CAST(7 AS Uint32?),
        CAST(-8 AS Int64?),
        CAST(9 AS Uint64?),
        CAST(10.10 AS Float?),
        CAST(11.11 AS Double?),
        CAST(12 AS Date?),
        CAST(13 AS Datetime?),
        CAST(14 AS Timestamp?),
        CAST(15 AS Interval?),
        CAST(16 AS TzDate?),
        CAST(17 AS TzDatetime?),
        CAST(18 AS TzTimestamp?),
        CAST(0xA19F AS Bytes?),
        CAST(0x20 AS Text?),
        CAST('<a=z;x=y>[{abc=123; def=456};{abc=234; xyz=789};]' AS YSON?),
        CAST('{"firstName": "John","lastName": "Smith","isAlive": true,"age": 27,"address": {"streetAddress": "21 2nd Street","postalCode": "10021-3100"},"phoneNumbers": [{"type": "office","number": "646 555-4567"}],"children": ["Catherine","Trevor"],"spouse": null}' AS JSON?),
        CAST('34ba4833-d48f-5655-8113-e247da8fe502' AS UUID?),
        CAST('{"firstName": "John","lastName": "Smith","isAlive": true,"age": 27,"address": {"streetAddress": "21 2nd Street","postalCode": "10021-3100"},"phoneNumbers": [{"type": "office","number": "646 555-4567"}],"children": ["Catherine","Trevor"],"spouse": null}' AS JSONDocument?),
        CAST("1E-130" AS DyNumber?)`
        const res = await queryRunner.query(query, [], true)
        expect(res).to.eq([1, "abc", 123.12])
    })
})
