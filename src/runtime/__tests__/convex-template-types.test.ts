import { expectTypeOf, it } from 'vitest';
import type { FunctionArgs } from 'convex/server';
import type { api } from '../../../templates/convex/_generated/api';
import type { Doc, Id } from '../../../templates/convex/_generated/dataModel';
import type { MutationCtx, QueryCtx } from '../../../templates/convex/_generated/server';

it('derives template contexts, documents and handler arguments from the schema', () => {
    expectTypeOf<MutationCtx>().not.toBeAny();
    expectTypeOf<QueryCtx['db']>().not.toBeAny();
    expectTypeOf<Doc<'workspace_members'>['role']>().toEqualTypeOf<'owner' | 'editor' | 'viewer'>();
    expectTypeOf<FunctionArgs<typeof api.workspaces.setActive>>().toEqualTypeOf<{
        workspace_id: Id<'workspaces'>;
    }>();
});

// This function is checked by tsc, never executed. These errors must remain
// errors: permissive generated stubs would make the directives fail compilation.
function rejectInvalidTemplateQueries(ctx: QueryCtx): void {
    // @ts-expect-error Table names must come from the schema.
    ctx.db.query('missing_table');
    // @ts-expect-error Index names must belong to the queried table.
    ctx.db.query('workspaces').withIndex('missing_index');
}
void rejectInvalidTemplateQueries;
