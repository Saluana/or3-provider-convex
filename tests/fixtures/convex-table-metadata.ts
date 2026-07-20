export function getPkField(tableName: string): string {
    return tableName === 'file_meta' ? 'hash' : 'id';
}
