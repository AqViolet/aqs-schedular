import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AQS · Scheduler operations",
  description:
    "Local scheduler operations, run history, and live command output.",
  robots: { index: false, follow: false },
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
