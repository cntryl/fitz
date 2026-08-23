import { For } from "@askrjs/askr/control";
import {
  Block,
  Card,
  CardContent,
  Section,
  Stat,
  StatDescription,
  StatLabel,
  StatValue,
  Text,
} from "@askrjs/themes/components";
import { formatDisplayValue } from "@/shared/format";

export interface DomainSummaryItem {
  caption?: string;
  label: string;
  value: string | number;
}

export interface DomainSummaryStripProps {
  ariaLabel?: string;
  class?: string;
  description?: string;
  id?: string;
  items: readonly DomainSummaryItem[];
  title?: string;
}

export default function DomainSummaryStrip({
  ariaLabel,
  class: className,
  description,
  id,
  items,
  title,
}: DomainSummaryStripProps) {
  const titleId = title
    ? (id ?? `${title.toLowerCase().replace(/[^a-z0-9]+/g, "-")}-title`)
    : undefined;

  return (
    <Section
      className={`domain-summary-strip${className ? ` ${className}` : ""}`}
      direction="column"
      gap="sm"
      paddingY="0"
      aria-label={ariaLabel ?? (title ? undefined : "Summary")}
      aria-labelledby={titleId}
    >
      {title || description ? (
        <Block as="header" className="domain-summary-header">
          {title ? <h2 id={titleId}>{title}</h2> : null}
          {description ? (
            <Text tone="muted" size="sm">
              {description}
            </Text>
          ) : null}
        </Block>
      ) : null}
      <Block className="domain-summary-items">
        <For each={items as DomainSummaryItem[]} by={(item) => item.label}>
          {(item) => (
            <Card class="domain-summary-item" padding="sm" variant="default">
              <CardContent>
                <Stat>
                  <StatLabel>{item.label}</StatLabel>
                  <StatValue>
                    <Text as="strong" font="mono" numeric="tabular" size="lg" weight="semibold">
                      {formatDisplayValue(item.value)}
                    </Text>
                  </StatValue>
                  {item.caption ? <StatDescription>{item.caption}</StatDescription> : null}
                </Stat>
              </CardContent>
            </Card>
          )}
        </For>
      </Block>
    </Section>
  );
}
