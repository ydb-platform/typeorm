import { BaseDataSourceOptions } from "../../data-source/BaseDataSourceOptions"

/**
 * YDB specific connection options.
 */
export interface YdbConnectionOptions extends BaseDataSourceOptions {
    /**
     * Database type.
     */
    readonly type: "ydb"
    
    // TODO: add all options and checkmark if it used or not

    /**
     * Database name to connect to.
     * 
     * **NOT USED YET**, just to avoid failing the tests
     */
     readonly database?: string
}
