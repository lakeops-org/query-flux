import { getRoutingConfig, listGroupConfigs } from "@/lib/api";
import { RoutingEditor } from "./routing-editor";

export const revalidate = 0;

export default async function RoutingPage() {
  const [routing, groupConfigs] = await Promise.all([
    getRoutingConfig().catch(() => null),
    listGroupConfigs().catch(() => []),
  ]);

  const groups = groupConfigs.map((g) => ({ id: g.id, name: g.name }));

  return <RoutingEditor initialRouting={routing} groups={groups} />;
}
