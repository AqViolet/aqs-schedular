"use client";

import { useCallback, useEffect, useState } from "react";
import { api, messageOf } from "./api";

type Snapshot<T> = {
  path: string;
  data: T | null;
  error: string | null;
  updatedAt: Date | null;
};

export function useResource<T>(path: string | null, pollMs = 0) {
  const [snapshot, setSnapshot] = useState<Snapshot<T> | null>(null);
  const [version, setVersion] = useState(0);
  const refresh = useCallback(() => setVersion((value) => value + 1), []);

  useEffect(() => {
    if (!path) return;
    let disposed = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const controller = new AbortController();
    async function load() {
      try {
        const data = await api<T>(path!, { signal: controller.signal });
        if (!disposed)
          setSnapshot({
            path: path!,
            data,
            error: null,
            updatedAt: new Date(),
          });
      } catch (error) {
        if (!disposed)
          setSnapshot((previous) => ({
            path: path!,
            data: previous?.path === path ? previous.data : null,
            updatedAt: previous?.path === path ? previous.updatedAt : null,
            error: messageOf(error),
          }));
      } finally {
        if (!disposed && pollMs) timer = setTimeout(load, pollMs);
      }
    }
    void load();
    return () => {
      disposed = true;
      controller.abort();
      clearTimeout(timer);
    };
  }, [path, pollMs, version]);

  const current = snapshot?.path === path ? snapshot : null;
  return {
    data: current?.data ?? null,
    error: current?.error ?? null,
    updatedAt: current?.updatedAt ?? null,
    loading: path !== null && !current,
    refresh,
  };
}
