"use client";

import { getStudioEngineModule } from "@/lib/studio-engines/manifest";
import type { EngineDescriptor } from "@/lib/engine-registry";
import { GenericEngineClusterConfig } from "./generic-engine-cluster-config";
import { STUDIO_CUSTOM_CLUSTER_FORMS } from "./studio-engine-forms";
import type { FlatClusterConfig, PatchClusterConfig } from "./types";

export function EngineClusterConfig({
  engineKey,
  descriptor,
  flat,
  onPatch,
  readOnlyFieldKeys,
  hiddenFieldKeys,
}: {
  engineKey: string;
  descriptor: EngineDescriptor;
  flat: FlatClusterConfig;
  onPatch: PatchClusterConfig;
  readOnlyFieldKeys?: ReadonlySet<string>;
  hiddenFieldKeys?: ReadonlySet<string>;
}) {
  const mod = getStudioEngineModule(engineKey);
  const id = mod?.customFormId;
  if (id) {
    const Form = STUDIO_CUSTOM_CLUSTER_FORMS[id];
    if (Form) {
      return <Form flat={flat} onPatch={onPatch} />;
    }
  }
  return (
    <GenericEngineClusterConfig
      descriptor={descriptor}
      flat={flat}
      onPatch={onPatch}
      readOnlyFieldKeys={readOnlyFieldKeys}
      hiddenFieldKeys={hiddenFieldKeys}
    />
  );
}
