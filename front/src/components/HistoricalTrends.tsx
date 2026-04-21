import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { historicalData } from "@/lib/mockData";

const colors: Record<string, string> = {
  Python: "hsl(45 90% 55%)",
  TypeScript: "hsl(210 90% 55%)",
  Rust: "hsl(20 85% 55%)",
  Go: "hsl(190 70% 50%)",
  Java: "hsl(0 70% 55%)",
};

const HistoricalTrends = () => (
  <div className="panel-batch rounded-lg bg-card p-4 h-full">
    <div className="flex items-center justify-between mb-3">
      <h2 className="text-lg font-semibold text-foreground">Historical Trends</h2>
      <span className="text-xs font-medium text-secondary bg-secondary/10 px-2 py-0.5 rounded-full">BATCH</span>
    </div>
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={historicalData} margin={{ left: 0, right: 10 }}>
        <XAxis dataKey="month" tick={{ fill: "hsl(215 15% 55%)", fontSize: 11 }} />
        <YAxis tick={{ fill: "hsl(215 15% 55%)", fontSize: 11 }} />
        <Tooltip contentStyle={{ background: "hsl(224 18% 14%)", border: "1px solid hsl(224 15% 22%)", borderRadius: 8, color: "hsl(210 20% 92%)" }} />
        <Legend wrapperStyle={{ fontSize: 11 }} />
        {Object.entries(colors).map(([key, color]) => (
          <Line key={key} type="monotone" dataKey={key} stroke={color} strokeWidth={2} dot={false} />
        ))}
      </LineChart>
    </ResponsiveContainer>
    <p className="text-xs text-muted-foreground mt-2">Computed by PySpark from GH Archive — refreshed weekly.</p>
  </div>
);

export default HistoricalTrends;
