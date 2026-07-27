import { Brand, BrandLabel } from "@askrjs/themes/components";

export default function AuthBrand() {
  return (
    <Brand>
      <img
        class="fitz-brand-logo"
        src="/assets/logos/fitz-logo-128x128.png"
        alt=""
        aria-hidden="true"
      />
      <BrandLabel>Fitz Admin</BrandLabel>
    </Brand>
  );
}
