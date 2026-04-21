import { useEffect, useState } from "react";
import { randomEvent } from "@/lib/mockData";

interface FeedEvent {
  id: number;
  timestamp: string;
  type: string;
  repo: string;
}

let eventId = 0;

const ActivityFeed = () => {
  const [events, setEvents] = useState<FeedEvent[]>([]);

  useEffect(() => {
    // seed initial
    const seed = Array.from({ length: 5 }, () => ({ id: eventId++, ...randomEvent() }));
    setEvents(seed);

    const interval = setInterval(() => {
      setEvents((prev) => {
        const next = [{ id: eventId++, ...randomEvent() }, ...prev];
        return next.slice(0, 15);
      });
    }, 2000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="panel-stream rounded-lg bg-card p-4 h-full">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-foreground">Live Activity Feed</h2>
        <span className="flex items-center gap-1.5 text-xs font-medium text-primary bg-primary/10 px-2 py-0.5 rounded-full">
          <span className="w-2 h-2 rounded-full bg-primary animate-pulse-green" />
          LIVE
        </span>
      </div>
      <p className="text-xs text-muted-foreground mb-3">Raw Kafka events before Spark aggregation</p>
      <div className="space-y-0.5 max-h-[360px] overflow-y-auto font-mono text-xs">
        {events.map((e) => (
          <div key={e.id} className="flex items-center gap-3 px-2 py-1.5 rounded hover:bg-muted/50 transition-colors">
            <span className="text-muted-foreground w-20 shrink-0">{e.timestamp}</span>
            <span className="w-28 shrink-0">{e.type}</span>
            <span className="text-foreground truncate">{e.repo}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

export default ActivityFeed;
