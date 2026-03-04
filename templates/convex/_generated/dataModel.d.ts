export type Id<TableName extends string = string> = string & {
    readonly __tableName?: TableName;
};

export type TableNames = string;
