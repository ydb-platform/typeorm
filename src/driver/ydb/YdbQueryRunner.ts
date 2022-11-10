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

export class YdbQueryRunner extends BaseQueryRunner implements QueryRunner {
    constructor(ydbDriver: YdbDriver, replicationMode: ReplicationMode) {
        super()
    }

    query(
        query: string,
        parameters?: any[] | undefined,
        useStructuredResult?: boolean | undefined,
    ): Promise<any> {
        throw new Error("Method not implemented.")
    }

    protected loadTables(tablePaths?: string[] | undefined): Promise<Table[]> {
        throw new Error("Method not implemented.")
    }

    protected loadViews(tablePaths?: string[] | undefined): Promise<View[]> {
        throw new Error("Method not implemented.")
    }

    connect(): Promise<any> {
        throw new Error("Method not implemented.")
    }

    release(): Promise<void> {
        throw new Error("Method not implemented.")
    }

    clearDatabase(database?: string | undefined): Promise<void> {
        throw new Error("Method not implemented.")
    }

    startTransaction(
        isolationLevel?: IsolationLevel | undefined,
    ): Promise<void> {
        throw new Error("Method not implemented.")
    }

    commitTransaction(): Promise<void> {
        throw new Error("Method not implemented.")
    }

    rollbackTransaction(): Promise<void> {
        throw new Error("Method not implemented.")
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
