import { getGuardrailsConfig, listUserScripts } from "@/lib/api";
import { GuardrailsEditor } from "./guardrails-editor";

export const revalidate = 0;

export default async function GuardrailsPage() {
  const [config, scripts] = await Promise.all([
    getGuardrailsConfig().catch(() => null),
    listUserScripts("guard").catch(() => []),
  ]);
  return <GuardrailsEditor initialConfig={config} initialScripts={scripts} />;
}
