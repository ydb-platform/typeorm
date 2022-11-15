import { ObjectLiteral } from "../../common/ObjectLiteral"
import { ColumnMetadata } from "../../metadata/ColumnMetadata"
import { EntityMetadata } from "../../metadata/EntityMetadata"
import { QueryRunner } from "../../query-runner/QueryRunner"
import { SchemaBuilder } from "../../schema-builder/SchemaBuilder"
import { Table } from "../../schema-builder/table/Table"
import { TableColumn } from "../../schema-builder/table/TableColumn"
import { TableForeignKey } from "../../schema-builder/table/TableForeignKey"
import { View } from "../../schema-builder/view/View"
import { Driver, ReturningType } from "../Driver"
import { ColumnType } from "../types/ColumnTypes"
import { CteCapabilities } from "../types/CteCapabilities"
import { DataTypeDefaults } from "../types/DataTypeDefaults"
import { MappedColumnTypes } from "../types/MappedColumnTypes"
import { ReplicationMode } from "../types/ReplicationMode"
import { UpsertType } from "../types/UpsertType"
import * as Ydb from "ydb-sdk"
import { DataSource } from "../../data-source"
import { YdbConnectionOptions } from "./YdbConnectionOptions"
import { DriverPackageNotInstalledError } from "../../error"
import { YdbQueryRunner } from "./YdbQueryRunner"
import { RdbmsSchemaBuilder } from "../../schema-builder/RdbmsSchemaBuilder"

// TODO: remove Ydb references to have no direct deps
export class YdbDriver implements Driver {
    connection: DataSource
    options: YdbConnectionOptions
    version?: string | undefined
    database?: string | undefined
    schema?: string | undefined
    isReplicated: boolean
    treeSupport: boolean
    transactionSupport: "simple" | "nested" | "none"

    /**
     * We store all created query runners because we need to release them.
     */
    connectedQueryRunners: QueryRunner[] = []

    /**
     * Gets list of supported column data types by a driver.
     */
    /** @see https://ydb.tech/en/docs/yql/reference/types */
    //TODO: check all types
    supportedDataTypes: ColumnType[] = [
        "bool",
        "int32",
        "int64",
        "uint8",
        "uint32",
        "uint64",
        "float",
        "double",
        "string",
        "utf8",
        "json",
        "jsonDocument",
        "yson",
        "uuid",
        "date",
        "datetime",
        "timestamp",
        "interval",
        "TzDate",
        "TzDateTime",
        "TzTimestamp",
        "List",
        "Dictionary",
        "Set",
        "Tuple",
        "Structure",
        "Stream",
        "Variant",
        "Enumeration",
        "Callable",
        "Tagged",
        "Generic",
        "Unit",
        "Null",
        "Void",
        "EmptyList",
        "EmptyDict",
    ]
    supportedUpsertType?: UpsertType | undefined
    dataTypeDefaults: DataTypeDefaults
    spatialTypes: ColumnType[]
    withLengthColumnTypes: ColumnType[]
    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[] = ["decimal"] //TODO: add all precision types
    withScaleColumnTypes: ColumnType[]
    mappedDataTypes: MappedColumnTypes
    maxAliasLength?: number | undefined
    cteCapabilities: CteCapabilities

    // sdk specific vars
    /**
     * Ydb Sdk underlying library
     */
    Ydb: {
        Driver: typeof Ydb.Driver
    }

    /**
     * Ydb driver instance
     */
    driver: Ydb.Driver | undefined
    /**
     * Authentication service for Ydb Driver
     */
    authService: Ydb.IAuthService

    constructor(connection: DataSource) {
        this.connection = connection
        this.options = connection.options as YdbConnectionOptions
        this.transactionSupport = "simple"
        // load ydb-sdk package
        this.loadDependencies()
    }

    async connect(): Promise<void> {
        if (this.driver)
            this.connection.logger.log(
                "warn",
                "Multiple `connect` calls for one intsance of YdbDriver",
            )

        const options = this.options
        const driverOptions: Ydb.IDriverSettings = {
            authService: this.authService,
            endpoint: options.endpoint,
            database: options.database,
            poolSettings: {},
        }
        if (options.gRpcClientOptions)
            driverOptions.clientOptions = options.gRpcClientOptions
        if (options.poolSettings)
            driverOptions.poolSettings = options.poolSettings
        this.driver = new this.Ydb.Driver(driverOptions)

        await this.driver.ready(options.connectTimeout)
        // ready doesn't mean that connection is successfull (investigate why)
    }

    async afterConnect(): Promise<void> {
        //await this.driver?.tableClient.withSession(async (session) => {
        const qRunner: YdbQueryRunner = new YdbQueryRunner(this, "master")
        const result = await qRunner.query(`SELECT 1;`) //.session.executeQuery(`SELECT 1;`)
        console.log("Select 1", result)
        //})
    }

    async disconnect(): Promise<void> {
        await this.driver?.destroy()
        this.driver = undefined
    }
    createQueryRunner(mode: ReplicationMode): QueryRunner {
        return new YdbQueryRunner(this, mode)
    }
    createSchemaBuilder(): SchemaBuilder {
        // TODO: Needs implementation
        return new RdbmsSchemaBuilder(this.connection)
    }
    escapeQueryWithParameters(
        sql: string,
        parameters: ObjectLiteral,
        nativeParameters: ObjectLiteral,
    ): [string, any[]] {
        throw new Error("Method not implemented.")
    }
    escape(name: string): string {
        throw new Error("Method not implemented.")
    }
    /**
     * Build full table name with database name, schema name and table name.
     * E.g. myDB.mySchema.myTable
     */
    buildTableName(
        tableName: string,
        schema?: string | undefined,
        database?: string | undefined,
    ): string {
        let tablePath = [tableName]

        if (database) {
            tablePath.unshift(database)
        }

        return tablePath.join(".")
    }
    parseTableName(
        target: string | EntityMetadata | Table | View | TableForeignKey,
    ): {
        tableName: string
        schema?: string | undefined
        database?: string | undefined
    } {
        throw new Error("Method not implemented.")
    }
    preparePersistentValue(value: any, column: ColumnMetadata) {
        throw new Error("Method not implemented.")
    }
    prepareHydratedValue(value: any, column: ColumnMetadata) {
        throw new Error("Method not implemented.")
    }
    normalizeType(column: {
        type?:
            | string
            | BooleanConstructor
            | DateConstructor
            | NumberConstructor
            | StringConstructor
            | undefined
        length?: string | number | undefined
        precision?: number | null | undefined
        scale?: number | undefined
        isArray?: boolean | undefined
    }): string {
        //TODO: investigate all types should be converted (normalized)
        if (column.type === Number || column.type === "int") {
            return "int32"
        } else if (column.type === Date) {
            return "datetime"
        } else if (column.type === String) {
            return "string"
        } else if (column.type === Boolean) {
            return "bool"
        } else {
            return (column.type as string) || ""
        }
    }
    normalizeDefault(columnMetadata: ColumnMetadata): string | undefined {
        throw new Error("Method not implemented.")
    }
    normalizeIsUnique(column: ColumnMetadata): boolean {
        throw new Error("Method not implemented.")
    }
    getColumnLength(column: ColumnMetadata): string {
        throw new Error("Method not implemented.")
    }
    createFullType(column: TableColumn): string {
        throw new Error("Method not implemented.")
    }
    obtainMasterConnection(): Promise<any> {
        throw new Error("Method not implemented.")
    }
    obtainSlaveConnection(): Promise<any> {
        throw new Error("Method not implemented.")
    }
    createGeneratedMap(
        metadata: EntityMetadata,
        insertResult: any,
        entityIndex?: number | undefined,
        entityNum?: number | undefined,
    ): ObjectLiteral | undefined {
        throw new Error("Method not implemented.")
    }
    findChangedColumns(
        tableColumns: TableColumn[],
        columnMetadatas: ColumnMetadata[],
    ): ColumnMetadata[] {
        throw new Error("Method not implemented.")
    }
    isReturningSqlSupported(returningType: ReturningType): boolean {
        throw new Error("Method not implemented.")
    }
    isUUIDGenerationSupported(): boolean {
        throw new Error("Method not implemented.")
    }
    isFullTextColumnTypeSupported(): boolean {
        throw new Error("Method not implemented.")
    }
    createParameter(parameterName: string, index: number): string {
        throw new Error("Method not implemented.")
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------
    /**
     * Loads all driver dependencies.
     */
    protected loadDependencies(): void {
        try {
            // TODO: Change Ydb to this:
            // const lib = PlatformTools.load("ydb")

            this.Ydb = {
                Driver: Ydb.Driver,
            }
            const authOptions = this.options.authentication
            switch (authOptions.type) {
                case "anonymous":
                    this.authService = new Ydb.AnonymousAuthService()
                    break
                case "token":
                    this.authService = new Ydb.TokenAuthService(
                        authOptions.token,
                    )
                    break
                case "IAM":
                    const IamOptions = (({ type, ...o }) => o)(authOptions)
                    this.authService = new Ydb.IamAuthService(IamOptions)
                    break
                case "metadata":
                    if (authOptions.metadataTokenService) {
                        this.authService = new Ydb.MetadataAuthService(
                            authOptions.metadataTokenService,
                        )
                    } else {
                        this.authService = new Ydb.MetadataAuthService()
                    }
                    break

                default:
                    throw new TypeError(
                        `Ydb created with not defined auth type ${
                            (authOptions as any)?.type
                        }`,
                    )
            }
        } catch (e) {
            console.error(e)
            throw new DriverPackageNotInstalledError("Ydb", "ydb-sdk")
        }
    }
}
