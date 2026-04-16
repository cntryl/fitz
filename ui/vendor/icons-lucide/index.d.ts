import "@askrjs/askr/jsx-runtime";

export interface IconProps {
  size?: number | string;
  strokeWidth?: number;
  class?: string;
  title?: string;
  [key: string]: unknown;
}

export const Activity: (props?: IconProps) => JSX.Element;
export const ArrowRight: (props?: IconProps) => JSX.Element;
export const Gauge: (props?: IconProps) => JSX.Element;
export const LockKeyhole: (props?: IconProps) => JSX.Element;
export const LogOut: (props?: IconProps) => JSX.Element;
export const Shield: (props?: IconProps) => JSX.Element;
export const ShieldCheck: (props?: IconProps) => JSX.Element;
