import { expect } from "chai"
import { QueryResult } from "../../../src"

export const testFields = (
    res: QueryResult,
    callableProp: false | "valueOf" | "toString",
    check: any,
) => {
    return Object.entries(check).forEach(([key, value]) => {
        if (callableProp)
            expect(res.records[0][key][callableProp]()).to.be.equal(
                value,
                `in ${key}: ${res.records[0][key][callableProp]()}!=${value}`,
            )
        else
            expect(res.records[0][key]).to.be.equal(
                value,
                `in ${key}: ${res.records[0][key]}!=${value}`,
            )
    })
}
