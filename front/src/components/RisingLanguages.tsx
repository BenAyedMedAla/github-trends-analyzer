import { useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from "recharts";
import { generateLanguageStats } from "@/lib/mockData";

const RisingLanguages = () => {
  const [data, setData] = useState(generateLanguageStats);
  const [loading, setLoading] = useState(false);

  const runBatch = () => {
    setLoading(true);
    setTimeout(() => {
      setData(generateLanguageStats());
      setLoading(false);
    }, 2000);
  };

  return (
    <div className="panel-batch rounded-lg bg-card p-4 h-full">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-foreground">Rising Languages</h2>
        <span className="text-xs font-medium text-secondary bg-secondary/10 px-2 py-0.5 rounded-full">
          BATCH — updated Mondays
        </span>
      </div>
      <div className="flex items-center gap-3 mb-3">
        <button
          onClick={runBatch}
          disabled={loading}
          className="text-xs font-medium bg-secondary text-secondary-foreground px-3 py-1.5 rounded-md hover:opacity-90 disabled:opacity-50 transition-opacity"
        >
          {loading ? (
            <span className="flex items-center gap-1.5">
              <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Running...
            </span>
          ) : (
            "Run Batch Job"
          )}
        </button>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <BarChart data={data} layout="vertical" margin={{ left: 10, right: 10 }}>
          <XAxis type="number" tick={{ fill: "hsl(215 15% 55%)", fontSize: 11 }} />
          <YAxis dataKey="language" type="category" tick={{ fill: "hsl(210 20% 92%)", fontSize: 11 }} width={80} />
          <Tooltip contentStyle={{ background: "hsl(224 18% 14%)", border: "1px solid hsl(224 15% 22%)", borderRadius: 8, color: "hsl(210 20% 92%)" }} />
          <Legend wrapperStyle={{ fontSize: 11 }} />
          <Bar dataKey="thisWeek" name="This Week" fill="hsl(263 70% 55%)" radius={[0, 4, 4, 0]} />
          <Bar dataKey="lastWeek" name="Last Week" fill="hsl(263 40% 35%)" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

export default RisingLanguages;
