import {BaseEntity, Column, Entity, PrimaryColumn} from "../../../../src";

@Entity()
export class Simple extends BaseEntity {

    @PrimaryColumn()
    id: number

    @Column()
    title: string

    @Column({
        default: "This is default text.",
    })
    text: string

}
