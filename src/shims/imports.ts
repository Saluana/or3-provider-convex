/**
 * Shim for Nuxt's #imports virtual module.
 *
 * In a real Nuxt app, #imports re-exports all auto-imported composables.
 * This shim provides the subset that provider packages actually use,
 * sourced from their real origin modules.
 */
export { useRuntimeConfig, useNuxtApp, useFetch, useState } from 'nuxt/app';
