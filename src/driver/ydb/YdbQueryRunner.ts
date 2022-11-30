import { QueryRunner } from "../../query-runner/QueryRunner"
import { BaseQueryRunner } from "../../query-runner/BaseQueryRunner"
import { ReadStream } from "../../platform/PlatformTools"
import { Table } from "../../schema-builder/table/Table"
import { TableCheck } from "../../schema-builder/table/TableCheck"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableExclusion } from "../../schema-builder/table/TableExclusion"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { TableIndex } from "../../schema-builder/table/TableIndex"
import { TableUnique } from "../../schema-builder/table/TableUnique"
import { View } from "../../schema-builder/view/View"
import { IsolationLevel } from "../types/IsolationLevel"
import { YdbDriver } from "./YdbDriver"
import { ReplicationMode } from "../types/ReplicationMode"
import {
    DriverNotInitialized,
    QueryFailedError,
    QueryRunnerAlreadyReleasedError,
} from "../../error"
import { Broadcaster } from "../../subscriber/Broadcaster"
import * as Ydb from "ydb-sdk"

interface IQueryParams {
    [k: string]: Ydb.Ydb.ITypedValue
}

export class YdbQueryRunner extends BaseQueryRunner implements QueryRunner {
    /**
     * Database driver used by connection.
     */
    driver: YdbDriver

    /**
     * Ydb Sdk underlying library
     */
    Ydb: {
        Driver: typeof Ydb.Driver
        Session: typeof Ydb.Session
    }

    /**
     * Real database connection from a connection pool used to perform queries.
     */
    databaseConnection: Ydb.Session
    private sessionTimeout: number

    constructor(ydbDriver: YdbDriver, replicationMode: ReplicationMode) {
        super()
        this.driver = ydbDriver
        if (!this.driver.driver) {
            throw new DriverNotInitialized("ydb")
        }

        this.connection = ydbDriver.connection
        this.broadcaster = new Broadcaster(this)
        this.sessionTimeout = ydbDriver.options.connectTimeout
    }

    /**
     * Executes a given SQL query with optional parameters.
     */
    async query(
        query: string,
        parameters?: any[] | undefined,
        useStructuredResult?: boolean | undefined,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        this.driver.connection.logger.logQuery(query, parameters, this)

        const databaseConnection = await this.connect()

        let typedParams: IQueryParams = {}
        function isQueryParam(item: any): item is IQueryParams {
            return item[0]?.hasOwnProperty("type") === true
        }
        if (parameters?.length && isQueryParam(parameters[0])) {
            Object.assign(typedParams, parameters[0])
        } else {
            parameters?.map((val, ind) => {
                Object.assign(typedParams, {
                    ["$param" + ind.toString()]: Ydb.TypedValues.string(val),
                })
            })
        }

        try {
            const queryStartTime = +new Date()
            const result = await databaseConnection.executeQuery(
                query,
                typedParams,
            )

            // log slow queries if maxQueryExecution time is set
            const maxQueryExecutionTime =
                this.driver.options.maxQueryExecutionTime
            const queryEndTime = +new Date()
            const queryExecutionTime = queryEndTime - queryStartTime

            if (
                maxQueryExecutionTime &&
                queryExecutionTime > maxQueryExecutionTime
            ) {
                this.driver.connection.logger.logQuerySlow(
                    queryExecutionTime,
                    query,
                    parameters,
                    this,
                )
            }

            return result
        } catch (err) {
            this.driver.connection.logger.logQueryError(
                err,
                query,
                parameters,
                this,
            )
            throw new QueryFailedError(query, parameters, err)
        }
    }

    protected loadTables(tablePaths?: string[] | undefined): Promise<Table[]> {
        // TODO: Needs implementation
        if (tablePaths && tablePaths.length > 0) {
            throw new Error("Method not implemented.")
        } else {
            return Promise.resolve([])
        }
    }

    protected loadViews(tablePaths?: string[] | undefined): Promise<View[]> {
        // TODO: Needs implementation
        if (tablePaths && tablePaths.length > 0) {
            throw new Error("Method not implemented.")
        } else {
            return Promise.resolve([])
        }
    }

    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     * Returns obtained database connection.
     */
    async connect(): Promise<Ydb.Session> {
        if (!this.driver.driver) {
            throw new DriverNotInitialized("ydb")
        }
        if (this.databaseConnection && !this.isReleased)
            return this.databaseConnection

        if (!this.databaseConnection) {
            this.databaseConnection =
                await this.driver.driver.tableClient.getSessionUnmanaged(
                    this.sessionTimeout,
                )
        }

        this.databaseConnection.acquire()

        this.isReleased = false

        this.driver.connectedQueryRunners.push(this)

        return this.databaseConnection
    }

    /**
     * Releases used database connection.
     * You cannot use query runner methods once its released.
     */
    async release(): Promise<void> {
        if (this.isReleased || !this.databaseConnection) {
            return
        }

        this.isReleased = true

        this.databaseConnection.release()

        const index = this.driver.connectedQueryRunners.indexOf(this)

        if (index !== -1) {
            this.driver.connectedQueryRunners.splice(index, 1)
        }
    }
    async clearDatabase(database?: string | undefined): Promise<void> {
        const result = await this.driver.driver?.schemeClient.listDirectory("/")

        await this.driver.driver?.tableClient.withSession(async (session) => {
            result?.children.forEach((table) => {
                session.dropTable(table.name as string)
            })
        })
    }

    startTransaction(
        isolationLevel?: IsolationLevel | undefined,
    ): Promise<void> {
        // TODO: Needs implementation
        return Promise.resolve()
    }

    commitTransaction(): Promise<void> {
        // TODO: Needs implementation
        return Promise.resolve()
    }

    rollbackTransaction(): Promise<void> {
        // TODO: Needs implementation
        return Promise.resolve()
    }

    stream(
        query: string,
        parameters?: any[] | undefined,
        onEnd?: Function | undefined,
        onError?: Function | undefined,
    ): Promise<ReadStream> {
        throw new Error("Method not implemented.")
    }

    getDatabases(): Promise<string[]> {
        throw new Error("Method not implemented.")
    }

    getSchemas(database?: string | undefined): Promise<string[]> {
        throw new Error("Method not implemented.")
    }

    hasDatabase(database: string): Promise<boolean> {
        throw new Error("Method not implemented.")
    }

    getCurrentDatabase(): Promise<string | undefined> {
        throw new Error("Method not implemented.")
    }

    hasSchema(schema: string): Promise<boolean> {
        throw new Error("Method not implemented.")
    }

    getCurrentSchema(): Promise<string | undefined> {
        throw new Error("Method not implemented.")
    }

    hasTable(table: string | Table): Promise<boolean> {
        throw new Error("Method not implemented.")
    }

    hasColumn(table: string | Table, columnName: string): Promise<boolean> {
        throw new Error("Method not implemented.")
    }

    createDatabase(
        database: string,
        ifNotExist?: boolean | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropDatabase(
        database: string,
        ifExist?: boolean | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createSchema(
        schemaPath: string,
        ifNotExist?: boolean | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropSchema(
        schemaPath: string,
        ifExist?: boolean | undefined,
        isCascade?: boolean | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createTable(
        table: Table,
        ifNotExist?: boolean | undefined,
        createForeignKeys?: boolean | undefined,
        createIndices?: boolean | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropTable(
        table: string | Table,
        ifExist?: boolean | undefined,
        dropForeignKeys?: boolean | undefined,
        dropIndices?: boolean | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createView(
        view: View,
        syncWithMetadata?: boolean | undefined,
        oldView?: View | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropView(view: string | View): Promise<void> {
        throw new Error("Method not implemented.")
    }

    renameTable(
        oldTableOrName: string | Table,
        newTableName: string,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    addColumn(table: string | Table, column: TableColumn): Promise<void> {
        throw new Error("Method not implemented.")
    }

    addColumns(table: string | Table, columns: TableColumn[]): Promise<void> {
        throw new Error("Method not implemented.")
    }

    renameColumn(
        table: string | Table,
        oldColumnOrName: string | TableColumn,
        newColumnOrName: string | TableColumn,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    changeColumn(
        table: string | Table,
        oldColumn: string | TableColumn,
        newColumn: TableColumn,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    changeColumns(
        table: string | Table,
        changedColumns: { oldColumn: TableColumn; newColumn: TableColumn }[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropColumn(
        table: string | Table,
        column: string | TableColumn,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropColumns(
        table: string | Table,
        columns: TableColumn[] | string[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createPrimaryKey(
        table: string | Table,
        columnNames: string[],
        constraintName?: string | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    updatePrimaryKeys(
        table: string | Table,
        columns: TableColumn[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropPrimaryKey(
        table: string | Table,
        constraintName?: string | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createUniqueConstraint(
        table: string | Table,
        uniqueConstraint: TableUnique,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createUniqueConstraints(
        table: string | Table,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropUniqueConstraint(
        table: string | Table,
        uniqueOrName: string | TableUnique,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropUniqueConstraints(
        table: string | Table,
        uniqueConstraints: TableUnique[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createCheckConstraint(
        table: string | Table,
        checkConstraint: TableCheck,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createCheckConstraints(
        table: string | Table,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropCheckConstraint(
        table: string | Table,
        checkOrName: string | TableCheck,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropCheckConstraints(
        table: string | Table,
        checkConstraints: TableCheck[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createExclusionConstraint(
        table: string | Table,
        exclusionConstraint: TableExclusion,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createExclusionConstraints(
        table: string | Table,
        exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropExclusionConstraint(
        table: string | Table,
        exclusionOrName: string | TableExclusion,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropExclusionConstraints(
        table: string | Table,
        exclusionConstraints: TableExclusion[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createForeignKey(
        table: string | Table,
        foreignKey: TableForeignKey,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createForeignKeys(
        table: string | Table,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropForeignKey(
        table: string | Table,
        foreignKeyOrName: string | TableForeignKey,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropForeignKeys(
        table: string | Table,
        foreignKeys: TableForeignKey[],
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createIndex(table: string | Table, index: TableIndex): Promise<void> {
        throw new Error("Method not implemented.")
    }

    createIndices(table: string | Table, indices: TableIndex[]): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropIndex(
        table: string | Table,
        index: string | TableIndex,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    dropIndices(table: string | Table, indices: TableIndex[]): Promise<void> {
        throw new Error("Method not implemented.")
    }

    clearTable(tableName: string): Promise<void> {
        throw new Error("Method not implemented.")
    }
}
