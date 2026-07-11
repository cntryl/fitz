import { readFileSync, writeFileSync } from "node:fs";

const generatedPath = new URL("../src/adapters/generated/api.ts", import.meta.url);
const generated = readFileSync(generatedPath, "utf8").replace(/\s+$/u, "\n");

writeFileSync(generatedPath, generated);
