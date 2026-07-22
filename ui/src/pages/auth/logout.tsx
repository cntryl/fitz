import { state } from "@askrjs/askr";
import { task } from "@askrjs/askr/resources";
import { navigate } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/components";
import { Spinner } from "@askrjs/themes/components";
import {
  Alert,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import { createSignOutMutation } from "@/features/session/session-mutation";
import { manageRoutePageContext } from "@/components/shared/domain-page-frame";
import { formatUnknownError } from "@/shared/errors/format";

type LogoutPhase = "pending" | "success" | "open" | "error";

export default function Logout() {
  const signOut = createSignOutMutation();
  const [phase, setPhase] = state<LogoutPhase>("pending");
  const [error, setError] = state("");

  task(() => manageRoutePageContext("Sign out"));

  async function signOutAndSetPhase() {
    setPhase("pending");
    setError("");

    try {
      const result = await signOut.execute(undefined);
      setPhase(result?.performed === false ? "open" : "success");
    } catch (err) {
      setError(formatUnknownError(err));
      setPhase("error");
    }
  }

  task(async () => {
    await signOutAndSetPhase();
  });

  const currentPhase = phase();
  const errorMessage = error();
  const title =
    currentPhase === "open"
      ? "Open access"
      : currentPhase === "success"
        ? "Signed out"
        : currentPhase === "error"
          ? "Sign out failed"
          : "Signing out";
  const description =
    currentPhase === "open"
      ? "Admin authentication is disabled, so there is no browser account session to clear."
      : currentPhase === "success"
        ? "Your Fitz Admin session has been cleared."
        : currentPhase === "error"
          ? "We could not clear your session. You may still be signed in."
          : "Clearing your Fitz Admin session.";

  return (
    <Card class="auth-card" variant="raised">
      <CardHeader>
        <CardTitle titleAs="h1">{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="auth-status-shell" aria-live="polite" aria-atomic="true">
          {currentPhase === "pending" ? (
            <div class="auth-status">
              <Spinner label="Signing out" />
              <p>Clearing your session.</p>
            </div>
          ) : null}

          {currentPhase === "success" ? (
            <Alert
              variant="success"
              description="You can sign back in from here."
              actions={
                <Button onPress={() => navigate("/login", { history: "replace" })}>
                  Go to sign in
                </Button>
              }
            />
          ) : null}

          {currentPhase === "open" ? (
            <Alert
              variant="info"
              description="You can return directly to the open admin workspace."
              actions={
                <Button onPress={() => navigate("/admin", { history: "replace" })}>
                  Return to Fitz Admin
                </Button>
              }
            />
          ) : null}

          {currentPhase === "error" ? (
            <Alert
              variant="danger"
              description={`${errorMessage || "We could not clear your session."} Your session may still be active.`}
              actions={
                <Button variant="outline" onPress={() => void signOutAndSetPhase()}>
                  Try again
                </Button>
              }
            />
          ) : null}
        </div>
      </CardContent>
    </Card>
  );
}
