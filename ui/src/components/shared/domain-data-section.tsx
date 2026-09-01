import type { JSXElement } from "@askrjs/askr/foundations/structures";
import { Block, Section, Text } from "@askrjs/themes/components";

export interface DomainDataSectionProps {
  actions?: JSXElement | JSX.Element | null;
  children: JSXElement | JSX.Element;
  class?: string;
  description?: string;
  id: string;
  title: string;
}

export default function DomainDataSection({
  actions,
  children,
  class: className,
  description,
  id,
  title,
}: DomainDataSectionProps) {
  return (
    <Section
      className={`domain-data-section${className ? ` ${className}` : ""}`}
      aria-labelledby={id}
      direction="column"
      gap="sm"
      paddingY="0"
    >
      <Block as="header" className="domain-section-header">
        <Block direction="column" gap="xs">
          <h2 id={id}>{title}</h2>
          {description ? (
            <Text tone="muted" size="sm">
              {description}
            </Text>
          ) : null}
        </Block>
        {actions ? (
          <Block direction="row" align="center" gap="sm" wrap>
            {actions}
          </Block>
        ) : null}
      </Block>
      {children}
    </Section>
  );
}
