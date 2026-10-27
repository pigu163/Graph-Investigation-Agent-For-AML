import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

export function GlassPanel({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("glass-panel", className)} {...props} />;
}
