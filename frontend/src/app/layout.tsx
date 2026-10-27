import type { Metadata } from "next";
import { AppShell } from "@/components/app-shell";
import "./globals.css";

export const metadata: Metadata = {
  title: "Graph Investigation · AML Intelligence",
  description: "Frontend foundation for evidence-grounded AML and fraud investigations.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en" className="dark"><body><AppShell>{children}</AppShell></body></html>;
}
