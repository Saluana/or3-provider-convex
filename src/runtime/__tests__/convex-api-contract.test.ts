import { getFunctionName } from 'convex/server';
import { describe, expect, it } from 'vitest';
import {
    convexApiContractNames,
    convexInternalApiContractNames,
    getConvexApiReference,
    getConvexInternalApiReference,
} from '../utils/convex-api';

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

    it('resolves every internal function reference to the expected name', () => {
        for (const expectedName of convexInternalApiContractNames) {
            const reference = getConvexInternalApiReference(expectedName);
            expect(getFunctionName(reference)).toBe(expectedName);
        }
    });

    it('keeps internal functions out of the public contract', () => {
        const publicNames = new Set<string>(convexApiContractNames);
        for (const internalName of convexInternalApiContractNames) {
            expect(publicNames.has(internalName)).toBe(false);
            expect(() => getConvexApiReference(internalName)).toThrow(
                `Internal Convex function is not public: ${internalName}`
            );
        }
    });
});
