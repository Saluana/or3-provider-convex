import { getFunctionName } from 'convex/server';
import { describe, expect, it } from 'vitest';
import { convexApiContractNames, getConvexApiReference } from '../utils/convex-api';

describe('convex api contract', () => {
    it('resolves every mapped function reference to the expected name', () => {
        for (const expectedName of convexApiContractNames) {
            const reference = getConvexApiReference(expectedName);
            expect(getFunctionName(reference)).toBe(expectedName);
        }
    });

    it('does not contain duplicate function names', () => {
        const names = [...convexApiContractNames];
        expect(new Set(names).size).toBe(names.length);
    });
});
