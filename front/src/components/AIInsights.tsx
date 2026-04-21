import { aiInsights } from "@/lib/mockData";

const AIInsights = () => (
  <div className="panel-batch rounded-lg bg-card p-4 h-full">
    <div className="flex items-center justify-between mb-3">
      <h2 className="text-lg font-semibold text-foreground">AI Insights</h2>
      <span className="text-xs font-medium text-secondary bg-secondary/10 px-2 py-0.5 rounded-full">BATCH + ML</span>
    </div>
    <div className="space-y-2">
      {aiInsights.map((item) => (
        <div key={item.repo} className="bg-muted/50 rounded-lg p-3 flex items-center gap-4">
          <div className="flex-1 min-w-0">
            <p className="font-medium text-sm text-foreground truncate">{item.repo}</p>
            <p className="text-xs text-muted-foreground">{item.cluster}</p>
          </div>
          <div className="text-right shrink-0">
            <p className="text-sm font-semibold text-primary">{Math.round(item.probability * 100)}%</p>
            <p className="text-xs text-muted-foreground">+{item.predictedStars} ⭐ next wk</p>
          </div>
        </div>
      ))}
    </div>
    <p className="text-xs text-muted-foreground mt-3">Scored by Random Forest — refreshed weekly.</p>
  </div>
);

export default AIInsights;
