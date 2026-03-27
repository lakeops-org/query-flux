import { getSecurityConfig } from "@/lib/api";
import { SecurityEditor } from "./security-editor";

export const revalidate = 0;

export default async function SecurityPage() {
  const security = await getSecurityConfig().catch(() => null);

  return <SecurityEditor initialSecurity={security} />;
}
