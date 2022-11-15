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
    QueryFailedError,
    QueryRunnerAlreadyReleasedError,
    TransactionNotStartedError,
} from "../../error"
import { Broadcaster } from "../../subscriber/Broadcaster"
import * as Ydb from "ydb-sdk"
import convertYdbTypeToObject from "./convertYdbTypeToObject"

//TODO: make all methods uses Ydb.Session, retryable
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
    private currenTransaction: { id: string } = { id: "" }

    constructor(ydbDriver: YdbDriver, replicationMode: ReplicationMode) {
        super()
        this.driver = ydbDriver
        this.connection = ydbDriver.connection
        this.broadcaster = new Broadcaster(this)
    }
    /**
     * Executes a given SQL query with optional parameters.
     */
    async query(
        query: string,
        /*
         TODO: check from where parameters can be send.
               If it used outside the driver it must be Array and should be converted
        */
        parameters?: any | undefined,
        useStructuredResult?: boolean | undefined,
    ): Promise<any> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        this.driver.connection.logger.logQuery(query, parameters, this)

        const databaseConnection = await this.connect()

        try {
            const queryStartTime = +new Date()
            const result = await databaseConnection.executeQuery(
                query,
                parameters,
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

    /**
     * Loads all tables (with given names) from the database and creates a Table from them.
     */
    protected async loadTables(
        tableNames?: string[] | undefined,
    ): Promise<Table[]> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const databaseConnection = await this.connect()

        const loadAllTables =
            (tableNames && tableNames.length === 0) || !tableNames

        const result = await this.getSchemaList("", true)

        const schemeList = result.filter(
            (item) =>
                item.type === Ydb.Ydb.Scheme.Entry.Type.TABLE &&
                (tableNames?.includes(item.name || "") ||
                    ((item.path as string).indexOf(".sys") === -1 &&
                        loadAllTables)),
        )

        let tables: Table[] = []

        for await (const schemeItem of schemeList || []) {
            const table: Table = new Table({
                name: schemeItem.name,
                schema: schemeItem.path,
                database: this.driver.database,
            })

            const findAttr = function (obj: any, strKey: string): any {
                return [].concat.apply(
                    [],
                    Object.keys(obj).map(function (key) {
                        if (typeof obj[key] === "object")
                            return findAttr(obj[key], strKey)
                        if (key === strKey) return obj[key]
                    }),
                )
            }

            const tableDescription: Ydb.Ydb.Table.DescribeTableResult =
                await databaseConnection.describeTable(schemeItem.path)
            tableDescription.columns.forEach((column, index) => {
                let typeName: any
                if (column.type) typeName = convertYdbTypeToObject(column.type)

                table.columns.push(
                    new TableColumn({
                        name: column.name || "",
                        type: typeName.inner.type.toLowerCase() || "",
                        isPrimary:
                            column.name ===
                                tableDescription.primaryKey.find(
                                    (value) => value === column.name,
                                ) || false,
                    }),
                )
            })

            tables.push(table)
        }

        return tables
    }

    protected async loadViews(
        tablePaths?: string[] | undefined,
    ): Promise<View[]> {
        //it seems to be unsupported in Ydb
        return [new View()]
    }

    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     * Returns obtained database connection.
     */
    async connect(): Promise<any> {
        if (this.databaseConnection && !this.isReleased)
            return this.databaseConnection

        if (!this.databaseConnection) {
            this.databaseConnection =
                (await this.driver.driver?.tableClient.getSessionUnmanaged(
                    2000,
                )) as Ydb.Session
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
        if (this.isReleased) {
            return
        }

        this.isReleased = true

        this.databaseConnection?.release()

        const index = this.driver.connectedQueryRunners.indexOf(this)

        if (index !== -1) {
            this.driver.connectedQueryRunners.splice(index, 1)
        }
    }

    /**
     * Removes all tables from the currently connected database.
     */
    async clearDatabase(database?: string | undefined): Promise<void> {
        if (!this.loadedTables || this.loadedTables.length === 0)
            this.loadedTables = await this.getTables()
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const databaseConnection = await this.connect()

        this.loadedTables.forEach((table) => {
            databaseConnection.dropTable(`${table.schema}/${table.name}`)
        })
    }

    /**
     * Starts transaction.
     */
    async startTransaction(
        isolationLevel?: IsolationLevel | undefined,
    ): Promise<void> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()

        const databaseConnection = await this.connect()

        try {
            this.isTransactionActive = true
            try {
                await this.broadcaster.broadcast("BeforeTransactionStart")
            } catch (err) {
                this.isTransactionActive = false
                throw err
            }

            if (this.transactionDepth === 0) {
                if (!isolationLevel || isolationLevel === "SERIALIZABLE") {
                    const txMeta = await databaseConnection.beginTransaction({
                        serializableReadWrite: {},
                    })
                    this.currenTransaction.id = txMeta.id
                } else {
                    throw new Error(
                        "Only SERIALIZABLE isolation level is supported by Ydb driver",
                    )
                }
            } else {
                throw new Error(
                    "Nested transactions is not supported by Ydb driver",
                )
            }

            this.transactionDepth += 1

            await this.broadcaster.broadcast("AfterTransactionStart")
        } catch (err) {
            this.isTransactionActive = false
            throw err
        }
    }

    /**
     * Commits transaction.
     * Error will be thrown if transaction was not started.
     */
    async commitTransaction(): Promise<void> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()
        if (!this.isTransactionActive) throw new TransactionNotStartedError()

        const databaseConnection = await this.connect()

        try {
            await this.broadcaster.broadcast("BeforeTransactionCommit")

            await databaseConnection.commitTransaction({
                txId: this.currenTransaction.id,
            })

            this.isTransactionActive = false
            this.transactionDepth -= 1

            await this.broadcaster.broadcast("AfterTransactionCommit")
        } catch (err) {
            this.isTransactionActive = false
            this.transactionDepth -= 1
            throw err
        }
    }

    /**
     * Rollbacks transaction.
     * Error will be thrown if transaction was not started.
     */
    async rollbackTransaction(): Promise<void> {
        if (this.isReleased) throw new QueryRunnerAlreadyReleasedError()
        if (!this.isTransactionActive) throw new TransactionNotStartedError()

        const databaseConnection = await this.connect()

        try {
            await this.broadcaster.broadcast("BeforeTransactionRollback")

            await databaseConnection.rollbackTransaction({
                txId: this.currenTransaction.id,
            })

            this.isTransactionActive = false
            this.transactionDepth -= 1

            await this.broadcaster.broadcast("AfterTransactionRollback")
        } catch (err) {
            this.isTransactionActive = false
            this.transactionDepth -= 1
            throw err
        }
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
    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------
    /**
     *  Gets schema list
     */
    protected async getSchemaList(
        path: string,
        recursively: boolean = false,
    ): Promise<any[]> {
        const schemeList = await this.driver.driver?.schemeClient.listDirectory(
            path,
        )

        interface ISchemaListItem {
            path: string
            name: string
            type: Ydb.Ydb.Scheme.Entry.Type
        }

        let result: ISchemaListItem[] = []

        for await (const schemeItem of schemeList?.children || []) {
            result.push({
                path: `${path}`,
                name: schemeItem.name || "",
                type:
                    schemeItem.type ||
                    Ydb.Ydb.Scheme.Entry.Type.TYPE_UNSPECIFIED,
            })

            if (
                schemeItem.type === Ydb.Ydb.Scheme.Entry.Type.DIRECTORY &&
                recursively
            ) {
                const layerResult = await this.getSchemaList(
                    `${path}/${schemeItem.name}`,
                    recursively,
                )

                result.push(...layerResult)
            }
        }

        return result
    }
}
