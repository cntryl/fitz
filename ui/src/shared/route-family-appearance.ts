const ROUTE_FAMILY_ICON_SLOTS = [
  "identity-0",
  "identity-1",
  "identity-2",
  "identity-3",
  "identity-4",
] as const;

export function routeFamilyIconSlot(routeFamilyId: string) {
  let hash = 0;

  for (const character of routeFamilyId) {
    hash = (Math.imul(hash, 31) + character.charCodeAt(0)) >>> 0;
  }

  return ROUTE_FAMILY_ICON_SLOTS[hash % ROUTE_FAMILY_ICON_SLOTS.length];
}
