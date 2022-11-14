import "reflect-metadata"

import { expect } from "chai"
import { DataSource } from "../../../../src/data-source/DataSource"
import {
    closeTestingConnections,
    createTestingConnections,
    reloadTestingDatabases,
} from "../../../utils/test-utils"

describe("ydb driver > startup", () => {
    let connections: DataSource[]

    before(
        async () =>
            (connections = await createTestingConnections({
                entities: [], //__dirname + "/../entity/*{.js,.ts}"
                schemaCreate: true,
                dropSchema: false,
                enabledDrivers: ["ydb"],
            })),
    )
    beforeEach(() => reloadTestingDatabases(connections))
    after(() => closeTestingConnections(connections))

    it("should just startup", () =>
        Promise.all(
            connections.map(async (connection) => {
                // if we come this far, test was successful as a connection was established
                expect(connection).to.not.be.null
            }),
        ))
})
