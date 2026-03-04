export type MutationCtx = any;
export type QueryCtx = any;

type MutationDef = {
    args?: unknown;
    handler: (ctx: any, args: any) => any;
};

type QueryDef = {
    args?: unknown;
    handler: (ctx: any, args: any) => any;
};

export declare function mutation<T extends MutationDef>(definition: T): unknown;
export declare function query<T extends QueryDef>(definition: T): unknown;
export declare function internalMutation<T extends MutationDef>(definition: T): unknown;
export declare function internalQuery<T extends QueryDef>(definition: T): unknown;
export declare function action<T extends { args?: unknown; handler: (ctx: any, args: any) => any }>(definition: T): unknown;
export declare function internalAction<T extends { args?: unknown; handler: (ctx: any, args: any) => any }>(definition: T): unknown;
