import {expect} from "chai"
import Long from "long";

export const testFields = (
    res: any,
    callableProp: false | "valueOf" | "toString",
    check: any,
) => {
    return Object.entries(check).forEach(([key, value]) => {
        if (Long.isLong(res[key]) && Long.isLong(value)) {
            expect(res[key].compare(value)).to.eq(0,
                `in ${key}: ${res[key].toString()}!=${value.toString()}`)
        } else if (callableProp) {
            expect(res[key][callableProp]()).to.be.equal(
                value,
                `in ${key}: ${res[key][callableProp]()}!=${value}`,
            )
        } else if (res[key] instanceof Buffer && value instanceof Buffer) {
            expect(areBuffersEqual(res[key], value)).to.eq(true,
                `in ${key}: ${res[key]}!=${value}`)
        } else {
            expect(res[key]).to.be.equal(
                value,
                `in ${key}: ${res[key]}!=${value}`,
            )
        }
    })
}

function areBuffersEqual(bufA: Buffer, bufB: Buffer) {
    let len = bufA.length;
    if (len !== bufB.length) {
        return false;
    }
    for (let i = 0; i < len; i++) {
        if (bufA.readUInt8(i) !== bufB.readUInt8(i)) {
            return false;
        }
    }
    return true;
}
