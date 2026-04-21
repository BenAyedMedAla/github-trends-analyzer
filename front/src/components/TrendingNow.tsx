import { useEffect, useState, useRef } from "react";
import { initialTrendingRepos, TrendingRepo } from "@/lib/mockData";

const TrendingNow = () => {
  const [repos, setRepos] = useState<TrendingRepo[]>(initialTrendingRepos);
  const [flashedIndices, setFlashedIndices] = useState<Set<number>>(new Set());
  const flashKeyRef = useRef(0);

  useEffect(() => {
    const interval = setInterval(() => {
      setRepos((prev) => {
        const next = prev.map((r) => ({ ...r }));
        const count = Math.random() > 0.5 ? 2 : 1;
        const bumped = new Set<number>();
        for (let i = 0; i < count; i++) {
          const idx = Math.floor(Math.random() * next.length);
          next[idx].starsInWindow += Math.floor(Math.random() * 5) + 1;
          bumped.add(idx);
        }
        next.sort((a, b) => b.starsInWindow - a.starsInWindow);
        // find new indices of bumped repos
        const newFlashed = new Set<number>();
        bumped.forEach((oldIdx) => {
          const name = prev[oldIdx].name;
          const newIdx = next.findIndex((r) => r.name === name);
          if (newIdx >= 0) newFlashed.add(newIdx);
        });
        setFlashedIndices(newFlashed);
        flashKeyRef.current++;
        return next;
      });
      setTimeout(() => setFlashedIndices(new Set()), 1000);
    }, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="panel-stream rounded-lg bg-card p-4 h-full">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-foreground">Trending Now</h2>
        <span className="flex items-center gap-1.5 text-xs font-medium text-primary bg-primary/10 px-2 py-0.5 rounded-full">
          <span className="w-2 h-2 rounded-full bg-primary animate-pulse-green" />
          LIVE
        </span>
      </div>
      <p className="text-xs text-muted-foreground mb-3">Top repos by stars gained in last 10 min</p>
      <div className="space-y-1">
        {repos.map((repo, i) => (
          <div
            key={repo.name}
            className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm transition-colors ${
              flashedIndices.has(i) ? "flash-row" : ""
            }`}
          >
            <span className="text-muted-foreground font-mono w-5 text-right">{i + 1}</span>
            <span className="font-medium text-foreground flex-1 truncate">{repo.name}</span>
            <span className="text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded">{repo.language}</span>
            <span className="font-mono text-primary text-sm">⭐ {repo.starsInWindow}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default TrendingNow;
