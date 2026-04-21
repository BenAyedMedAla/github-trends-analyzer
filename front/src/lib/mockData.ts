export interface TrendingRepo {
  name: string;
  language: string;
  starsInWindow: number;
  flashing?: boolean;
}

export const initialTrendingRepos: TrendingRepo[] = [
  { name: "vercel/ai", language: "TypeScript", starsInWindow: 42 },
  { name: "ollama/ollama", language: "Go", starsInWindow: 38 },
  { name: "langchain-ai/langchain", language: "Python", starsInWindow: 35 },
  { name: "astral-sh/ruff", language: "Rust", starsInWindow: 31 },
  { name: "openai/whisper", language: "Python", starsInWindow: 28 },
  { name: "denoland/deno", language: "Rust", starsInWindow: 25 },
  { name: "tauri-apps/tauri", language: "Rust", starsInWindow: 22 },
  { name: "microsoft/typescript", language: "TypeScript", starsInWindow: 19 },
];

export interface LanguageStats {
  language: string;
  thisWeek: number;
  lastWeek: number;
}

export const generateLanguageStats = (): LanguageStats[] => [
  { language: "Python", thisWeek: rand(8000, 12000), lastWeek: rand(7000, 11000) },
  { language: "TypeScript", thisWeek: rand(6000, 10000), lastWeek: rand(5500, 9000) },
  { language: "Rust", thisWeek: rand(4000, 7000), lastWeek: rand(3500, 6500) },
  { language: "Go", thisWeek: rand(3500, 6000), lastWeek: rand(3000, 5500) },
  { language: "Java", thisWeek: rand(3000, 5500), lastWeek: rand(2800, 5000) },
  { language: "C++", thisWeek: rand(2000, 4000), lastWeek: rand(1800, 3800) },
  { language: "Swift", thisWeek: rand(1500, 3000), lastWeek: rand(1300, 2800) },
  { language: "Kotlin", thisWeek: rand(1200, 2500), lastWeek: rand(1000, 2300) },
];

function rand(min: number, max: number) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const repoPool = [
  "vercel/ai", "ollama/ollama", "langchain-ai/langchain", "astral-sh/ruff",
  "openai/whisper", "denoland/deno", "tauri-apps/tauri", "microsoft/typescript",
  "huggingface/transformers", "facebook/react", "sveltejs/svelte", "vuejs/core",
  "rust-lang/rust", "golang/go", "pytorch/pytorch", "tensorflow/tensorflow",
];

export function randomEvent() {
  const isStar = Math.random() > 0.35;
  return {
    timestamp: new Date().toLocaleTimeString(),
    type: isStar ? "⭐ WatchEvent" : "🍴 ForkEvent",
    repo: repoPool[Math.floor(Math.random() * repoPool.length)],
  };
}

const months = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"];

export const historicalData = months.map((month) => ({
  month,
  Python: rand(5000, 14000),
  TypeScript: rand(4000, 11000),
  Rust: rand(2000, 8000),
  Go: rand(2500, 7000),
  Java: rand(2000, 6000),
}));

export interface AIInsight {
  repo: string;
  probability: number;
  cluster: string;
  predictedStars: number;
}

export const aiInsights: AIInsight[] = [
  { repo: "vercel/ai", probability: 0.94, cluster: "Rocket repo 🚀", predictedStars: 2840 },
  { repo: "astral-sh/ruff", probability: 0.87, cluster: "Rocket repo 🚀", predictedStars: 2100 },
  { repo: "ollama/ollama", probability: 0.82, cluster: "Steady grower 📈", predictedStars: 1950 },
  { repo: "denoland/deno", probability: 0.68, cluster: "Steady grower 📈", predictedStars: 1200 },
  { repo: "cool-new-lib/xyz", probability: 0.45, cluster: "One-hit wonder 💥", predictedStars: 580 },
];
