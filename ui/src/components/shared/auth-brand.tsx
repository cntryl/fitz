import { Brand, BrandLabel, BrandMark } from "@askrjs/themes/components";
import fitzLogo from "@/assets/fitz-logo.png";

export default function AuthBrand() {
  return (
    <Brand>
      <BrandMark aria-hidden="true">
        <img class="fitz-brand-logo" src={fitzLogo} alt="" />
      </BrandMark>
      <BrandLabel>Fitz Admin</BrandLabel>
    </Brand>
  );
}
