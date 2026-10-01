import { Link } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/components";

export interface RowsRequestPromptProps {
  description: string;
  href: string;
  label: string;
}

/** Stands in for a data-row table until the operator explicitly asks to load it. */
export default function RowsRequestPrompt({ description, href, label }: RowsRequestPromptProps) {
  return (
    <div class="rows-request-prompt">
      <p>{description}</p>
      <Button asChild variant="outline">
        <Link href={href}>{label}</Link>
      </Button>
    </div>
  );
}
