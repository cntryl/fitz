export function routeFamilyIconColor(routeFamilyId: string) {
  let hash = 0;

  for (const character of routeFamilyId) {
    hash = (Math.imul(hash, 31) + character.charCodeAt(0)) >>> 0;
  }

  const hue = (Math.imul(hash, 137) + 210) % 360;
  return `hsl(${hue} 68% 48%)`;
}
