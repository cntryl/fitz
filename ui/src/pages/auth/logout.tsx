import { state } from "@askrjs/askr";
import { task } from "@askrjs/askr/resources";
import { navigate } from "@askrjs/askr/router";
import {
  Block,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Spinner,
  Text,
} from "@askrjs/themes/components";
import AuthBrand from "@/components/shared/auth-brand";
import { createSignOutMutation } from "@/features/session/session-mutation";
import { manageRoutePageContext } from "@/components/shared/domain-page-frame";
import { formatUnknownError } from "@/shared/errors/format";

type LogoutPhase = "pending" | "error";

export default function Logout() {
  const signOut = createSignOutMutation();
  const [phase, setPhase] = state<LogoutPhase>("pending");
  const [error, setError] = state("");

  task(() => manageRoutePageContext("Sign out"));
  task(() => signOutAndRedirect());

  async function signOutAndRedirect() {
    setPhase("pending");
    setError("");

    try {
      await signOut.execute(undefined);
      navigate("/login", { history: "replace" });
    } catch (err) {
      setError(formatUnknownError(err));
      setPhase("error");
    }
  }

  const currentPhase = phase();
  const errorMessage = error();
  const title = currentPhase === "error" ? "Sign out failed" : "Signing out";
  const description =
    currentPhase === "error"
      ? "We could not clear your session. You may still be signed in."
      : "Clearing your Fitz Admin session.";

  return (
    <Card variant="raised">
      <CardHeader>
        <AuthBrand />
        <CardTitle titleAs="h1">{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <Block direction="column" align="start" gap="md" aria-live="polite" aria-atomic="true">
          {currentPhase === "pending" ? <Spinner label="Signing out" /> : null}

          {currentPhase === "error" ? (
            <Block direction="column" align="start" gap="md" role="alert">
              <Text tone="danger" size="sm">
                {errorMessage || "We could not clear your session."} Your session may still be
                active.
              </Text>
              <Button variant="outline" onPress={() => void signOutAndRedirect()}>
                Try again
              </Button>
            </Block>
          ) : null}
        </Block>
      </CardContent>
    </Card>
  );
}
