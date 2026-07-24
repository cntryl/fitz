import { Brand, BrandLabel } from "@askrjs/themes/components";
import fitzLogo from "@/assets/fitz-logo.png";

export default function AuthBrand() {
  return (
    <Brand>
      <img class="fitz-brand-logo" src={fitzLogo} alt="" aria-hidden="true" />
      <BrandLabel>Fitz Admin</BrandLabel>
    </Brand>
  );
}
