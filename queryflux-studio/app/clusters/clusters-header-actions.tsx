"use client";

import { useState } from "react";
import { Plus } from "lucide-react";
import { AddClusterDialog } from "@/components/add-cluster-dialog";

export function ClustersHeaderActions() {
  const [addOpen, setAddOpen] = useState(false);

  return (
    <>
      <button
        type="button"
        onClick={() => setAddOpen(true)}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 shadow-sm transition-colors"
      >
        <Plus size={16} />
        Add cluster
      </button>
      <AddClusterDialog open={addOpen} onClose={() => setAddOpen(false)} />
    </>
  );
}
