import { state } from "@askrjs/askr";
import { CheckIcon, CopyIcon } from "@askrjs/lucide";
import { Button } from "@askrjs/themes/components";

export default function CopyTextButton({ label, text }: { label: string; text: string }) {
  const [copied, setCopied] = state(false);

  async function copyText() {
    if (!navigator.clipboard) return;

    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
    } catch {
      setCopied(false);
    }
  }

  return (
    <Button
      type="button"
      size="sm"
      variant="ghost"
      aria-label={copied() ? `${label} copied` : label}
      onPress={() => void copyText()}
    >
      {copied() ? (
        <CheckIcon size={14} aria-hidden="true" />
      ) : (
        <CopyIcon size={14} aria-hidden="true" />
      )}
      {copied() ? "Copied" : "Copy"}
    </Button>
  );
}
