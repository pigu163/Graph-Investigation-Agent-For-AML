"use client";

import { useEffect, useState, type ReactNode } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { MotionConfig, motion, useReducedMotion } from "framer-motion";
import { Activity, Layers, Menu, Network, ShieldCheck, Sparkles } from "lucide-react";

import { GlassPanel } from "@/components/glass-panel";
import { Button } from "@/components/ui/button";
import { workspaces } from "@/lib/workspaces";
import { cn } from "@/lib/utils";

const icons = { activity: Activity, shield: ShieldCheck, network: Network, sparkles: Sparkles, layers: Layers };

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const [navigationOpen, setNavigationOpen] = useState(false);
  const [opaque, setOpaque] = useState(false);
  const [reduceMotion, setReduceMotion] = useState(false);
  const systemReducedMotion = useReducedMotion();

  useEffect(() => {
    document.documentElement.dataset.opaque = String(opaque);
    document.documentElement.dataset.reducedMotion = String(reduceMotion);
  }, [opaque, reduceMotion]);

  return (
    <MotionConfig reducedMotion={reduceMotion ? "always" : "user"}>
      <a href="#main" className="fixed top-3 left-3 z-50 -translate-y-24 rounded-lg bg-primary px-4 py-2 text-primary-foreground focus:translate-y-0">Skip to content</a>
      <div className="mx-auto grid min-h-dvh max-w-[1800px] gap-5 p-4 md:p-6 lg:grid-cols-[232px_minmax(0,1fr)]">
        <GlassPanel className="self-start p-4 lg:sticky lg:top-6 lg:flex lg:min-h-[calc(100dvh-3rem)] lg:flex-col">
          <div className="flex items-center justify-between gap-3 px-2 py-3">
            <Link href="/" className="flex items-center gap-3 font-semibold tracking-tight" aria-label="Graph Investigation Agent home">
              <span className="grid size-9 place-items-center rounded-xl border border-white/20 bg-white/10"><Network className="size-5 text-primary" /></span>
              <span>Graph Investigation<span className="mt-0.5 block text-[10px] font-medium tracking-[.2em] text-muted-foreground uppercase">AML Intelligence</span></span>
            </Link>
            <Button variant="ghost" size="icon" className="lg:hidden" aria-label="Toggle navigation" aria-expanded={navigationOpen} aria-controls="workspace-nav" onClick={() => setNavigationOpen(!navigationOpen)}><Menu /></Button>
          </div>
          <nav id="workspace-nav" aria-label="Workspaces" className={cn("mt-8 space-y-2", navigationOpen ? "block" : "hidden lg:block")}>
            {workspaces.map(({ slug, label, icon }) => {
              const Icon = icons[icon];
              const href = slug ? `/${slug}` : "/";
              const active = pathname === href;
              return <Link key={href} href={href} aria-current={active ? "page" : undefined} onClick={() => setNavigationOpen(false)} className={cn("flex items-center gap-3 rounded-2xl px-3 py-3 text-sm transition-colors", active ? "border border-white/15 bg-white/10 text-foreground shadow-sm" : "border border-transparent text-muted-foreground hover:bg-white/5 hover:text-foreground")}><Icon className="size-4" aria-hidden="true" />{label}</Link>;
            })}
          </nav>
          <p className="mt-auto hidden px-3 pt-16 pb-2 text-xs leading-5 text-muted-foreground lg:block">Evidence before conclusions.<br />Frontend foundation · v0.1</p>
        </GlassPanel>
        <div className="min-w-0 space-y-8">
          <GlassPanel className="flex flex-wrap items-center justify-between gap-4 px-6 py-4">
            <div className="text-sm"><span className="mr-2 inline-block size-2 rounded-full bg-slate-400" aria-hidden="true" />Backend not connected</div>
            <div className="flex flex-wrap gap-4 text-xs text-muted-foreground">
              <label className="flex cursor-pointer items-center gap-2"><input type="checkbox" checked={opaque} onChange={(e) => setOpaque(e.target.checked)} className="size-4 accent-[#b3c7ff]" />Opaque surfaces</label>
              <label className="flex cursor-pointer items-center gap-2"><input type="checkbox" checked={reduceMotion} onChange={(e) => setReduceMotion(e.target.checked)} className="size-4 accent-[#b3c7ff]" />Reduce motion</label>
            </div>
          </GlassPanel>
          <motion.main id="main" initial={false} animate={{ opacity: 1 }} transition={{ duration: reduceMotion || systemReducedMotion ? 0 : 0.18 }} className="px-1 pb-8 md:px-3">{children}</motion.main>
        </div>
      </div>
    </MotionConfig>
  );
}
