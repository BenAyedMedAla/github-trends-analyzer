import TrendingNow from "@/components/TrendingNow";
import RisingLanguages from "@/components/RisingLanguages";
import ActivityFeed from "@/components/ActivityFeed";
import HistoricalTrends from "@/components/HistoricalTrends";
import AIInsights from "@/components/AIInsights";

const Index = () => (
  <div className="min-h-screen bg-background">
    {/* Header */}
    <header className="border-b border-border px-4 py-3 sm:px-6">
      <div className="max-w-7xl mx-auto flex flex-col sm:flex-row items-start sm:items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-bold text-foreground">GitHub Trends Analyzer</h1>
          <p className="text-xs text-muted-foreground">Real-time streaming + weekly batch pipeline dashboard</p>
        </div>
        <div className="flex items-center gap-4">
          <span className="flex items-center gap-1.5 text-xs font-medium text-foreground">
            Stream: LIVE
            <span className="w-2.5 h-2.5 rounded-full bg-primary animate-pulse-green" />
          </span>
          <span className="flex items-center gap-1.5 text-xs font-medium text-foreground">
            Batch: Last run Monday
            <span className="w-2.5 h-2.5 rounded-full" style={{ background: "hsl(45 90% 55%)" }} />
          </span>
        </div>
      </div>
    </header>

    {/* Grid */}
    <main className="max-w-7xl mx-auto p-4 sm:p-6 grid grid-cols-1 md:grid-cols-2 gap-4">
      <TrendingNow />
      <RisingLanguages />
      <ActivityFeed />
      <HistoricalTrends />
      <div className="md:col-span-2">
        <AIInsights />
      </div>
    </main>
  </div>
);

export default Index;
