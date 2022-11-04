import { ObjectLiteral } from "../../common/ObjectLiteral";
import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions";
import { ColumnMetadata } from "../../metadata/ColumnMetadata";
import { EntityMetadata } from "../../metadata/EntityMetadata";
import { QueryRunner } from "../../query-runner/QueryRunner";
import { SchemaBuilder } from "../../schema-builder/SchemaBuilder";
import { Table } from "../../schema-builder/table/Table";
import { TableColumn } from "../../schema-builder/table/TableColumn";
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey";
import { View } from "../../schema-builder/view/View";
import { Driver, ReturningType } from "../Driver";
import { ColumnType } from "../types/ColumnTypes";
import { CteCapabilities } from "../types/CteCapabilities";
import { DataTypeDefaults } from "../types/DataTypeDefaults";
import { MappedColumnTypes } from "../types/MappedColumnTypes";
import { ReplicationMode } from "../types/ReplicationMode";
import { UpsertType } from "../types/UpsertType";
import * as Ydb from "ydb-sdk"
import { DataSource } from "../../data-source";

export class YdbDriver implements Driver {
    options: BaseDataSourceOptions;
    version?: string | undefined;
    database?: string | undefined;
    schema?: string | undefined;
    isReplicated: boolean;
    treeSupport: boolean;
    transactionSupport: "simple" | "nested" | "none";
    supportedDataTypes: ColumnType[];
    supportedUpsertType?: UpsertType | undefined;
    dataTypeDefaults: DataTypeDefaults;
    spatialTypes: ColumnType[];
    withLengthColumnTypes: ColumnType[];
    withPrecisionColumnTypes: ColumnType[];
    withScaleColumnTypes: ColumnType[];
    mappedDataTypes: MappedColumnTypes;
    maxAliasLength?: number | undefined;
    cteCapabilities: CteCapabilities;

    constructor(connection: DataSource) {
        const driver = new Ydb.Driver({} as Ydb.IDriverSettings)
        console.log(driver)
    }

    connect(): Promise<void> {
        throw new Error("Method not implemented.");
    }
    afterConnect(): Promise<void> {
        throw new Error("Method not implemented.");
    }
    disconnect(): Promise<void> {
        throw new Error("Method not implemented.");
    }
    createSchemaBuilder(): SchemaBuilder {
        throw new Error("Method not implemented.");
    }
    createQueryRunner(mode: ReplicationMode): QueryRunner {
        throw new Error("Method not implemented.");
    }
    escapeQueryWithParameters(sql: string, parameters: ObjectLiteral, nativeParameters: ObjectLiteral): [string, any[]] {
        throw new Error("Method not implemented.");
    }
    escape(name: string): string {
        throw new Error("Method not implemented.");
    }
    buildTableName(tableName: string, schema?: string | undefined, database?: string | undefined): string {
        throw new Error("Method not implemented.");
    }
    parseTableName(target: string | EntityMetadata | Table | View | TableForeignKey): { tableName: string; schema?: string | undefined; database?: string | undefined; } {
        throw new Error("Method not implemented.");
    }
    preparePersistentValue(value: any, column: ColumnMetadata) {
        throw new Error("Method not implemented.");
    }
    prepareHydratedValue(value: any, column: ColumnMetadata) {
        throw new Error("Method not implemented.");
    }
    normalizeType(column: { type?: string | BooleanConstructor | DateConstructor | NumberConstructor | StringConstructor | undefined; length?: string | number | undefined; precision?: number | null | undefined; scale?: number | undefined; isArray?: boolean | undefined; }): string {
        throw new Error("Method not implemented.");
    }
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        throw new Error("Method not implemented.");
    }
    normalizeIsUnique(column: ColumnMetadata): boolean {
        throw new Error("Method not implemented.");
    }
    getColumnLength(column: ColumnMetadata): string {
        throw new Error("Method not implemented.");
    }
    createFullType(column: TableColumn): string {
        throw new Error("Method not implemented.");
    }
    obtainMasterConnection(): Promise<any> {
        throw new Error("Method not implemented.");
    }
    obtainSlaveConnection(): Promise<any> {
        throw new Error("Method not implemented.");
    }
    createGeneratedMap(metadata: EntityMetadata, insertResult: any, entityIndex?: number | undefined, entityNum?: number | undefined): ObjectLiteral | undefined {
        throw new Error("Method not implemented.");
    }
    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: ColumnMetadata[]): ColumnMetadata[] {
        throw new Error("Method not implemented.");
    }
    isReturningSqlSupported(returningType: ReturningType): boolean {
        throw new Error("Method not implemented.");
    }
    isUUIDGenerationSupported(): boolean {
        throw new Error("Method not implemented.");
    }
    isFullTextColumnTypeSupported(): boolean {
        throw new Error("Method not implemented.");
    }
    createParameter(parameterName: string, index: number): string {
        throw new Error("Method not implemented.");
    }
    
}
