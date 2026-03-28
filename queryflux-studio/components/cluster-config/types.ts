/** Flat key-value form state aligned with {@link toUpsertBody} / API snake fields via dialog mapping. */
export type FlatClusterConfig = Record<string, string>;

export type PatchClusterConfig = (patch: Record<string, string>) => void;
