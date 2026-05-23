import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const componentFiles = [
  "area-chart/area-chart.js",
  "bar-chart/bar-chart.js",
  "chart-legend/chart-legend.js",
  "donut-chart/donut-chart.js",
  "flame-graph/flame-graph.js",
  "heatmap/heatmap.js",
  "line-chart/line-chart.js",
  "sparkline/sparkline.js",
  "stacked-bar-chart/stacked-bar-chart.js",
  "timeline/timeline.js",
];

const chartsDistDir = join(process.cwd(), "node_modules", "@askrjs", "charts", "dist", "components");
const sourceImport = 'from "@askrjs/askr";';
const patchedImport = 'from "@askrjs/askr/control";';

for (const relativePath of componentFiles) {
  const filePath = join(chartsDistDir, relativePath);
  const original = readFileSync(filePath, "utf8");

  if (!original.includes(sourceImport)) {
    continue;
  }

  writeFileSync(filePath, original.replace(sourceImport, patchedImport));
}
