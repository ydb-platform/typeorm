import { Ydb, google } from "ydb-sdk-proto"

export default function convertYdbTypeToObject(type: Ydb.IType): any {
    if (type.typeId) {
        const label = Ydb.Type.PrimitiveTypeId[type.typeId]
        if (!label) {
            throw new Error(`Unknown PrimitiveTypeId: ${type.typeId}`)
        }
        return { type: label }
    } else if (type.decimalType) {
        return { type: "decimal" }
    } else if (type.optionalType) {
        const innerType = type.optionalType.item as Ydb.IType
        return {
            type: "optional",
            inner: convertYdbTypeToObject(innerType),
        }
    } else if (type.listType) {
        const innerType = type.listType.item as Ydb.IType
        return {
            type: "list",
            inner: convertYdbTypeToObject(innerType),
        }
    } else if (type.tupleType) {
        const types = type.tupleType.elements as Ydb.IType[]
        return {
            type: "list",
            inner: types.map((type) => convertYdbTypeToObject(type)),
        }
    } else if (type.structType) {
        const members = type.structType.members as Ydb.IStructMember[]
        const struct = {} as any
        members.forEach((member) => {
            const memberName = member.name as string
            const memberType = member.type as Ydb.IType
            struct[memberName] = convertYdbTypeToObject(memberType)
        })
        return {
            type: "struct",
            inner: struct,
        }
    } else if (type.dictType) {
        const keyType = type.dictType.key as Ydb.IType
        const payloadType = type.dictType.payload as Ydb.IType

        ///////// maybe fo something additional?
        return {
            type: "dict",
            inner: {
                [convertYdbTypeToObject(keyType)]:
                    convertYdbTypeToObject(payloadType),
            },
        }
    } else if (type.variantType) {
        if (type.variantType.tupleItems) {
            const elements = type.variantType.tupleItems.elements as Ydb.IType[]

            return {
                type: "variant",
                inner: {
                    type: "tuple",
                    inner: elements.map((type) => {
                        convertYdbTypeToObject(type)
                    }),
                },
            }
        } else if (type.variantType.structItems) {
            const members = type.variantType.structItems
                .members as Ydb.IStructMember[]

            return {
                type: "variant",
                inner: {
                    type: "struct",
                    inner: members.map((entity) => {
                        if (entity.type)
                            return {
                                name: entity.name,
                                type: convertYdbTypeToObject(entity.type),
                            }
                        else
                            return {
                                name: entity.name,
                                type: entity.type,
                            }
                    }),
                },
            }
        } else {
            throw new Error(
                "Either tupleItems or structItems should be present in VariantType!",
            )
        }
    } else if (type.voidType === google.protobuf.NullValue.NULL_VALUE) {
        return { type: "null" }
    } else {
        throw new Error(`Unknown type ${JSON.stringify(type)}`)
    }
}
