import { state } from "@askrjs/askr";
import { task } from "@askrjs/askr/resources";
import { navigate } from "@askrjs/askr/router";
import { Button } from "@askrjs/themes/controls";
import { Spinner } from "@askrjs/themes/feedback";
import {
  Alert,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { createSignOutMutation } from "@/features/session/session-mutation";
import { formatUnknownError } from "@/shared/errors/format";

export default function Logout() {
  const signOut = createSignOutMutation();
  const [error, setError] = state("");

  async function signOutAndRedirect() {
    try {
      setError("");
      await signOut.execute(undefined);
      navigate("/login", { history: "replace" });
    } catch (err) {
      setError(formatUnknownError(err));
    }
  }

  task(async () => {
    await signOutAndRedirect();
  });

  return (
    <Card class="auth-card" variant="raised">
      <CardHeader>
        <CardTitle>Signing out</CardTitle>
        <CardDescription>Ending your Fitz admin session and returning to login.</CardDescription>
      </CardHeader>
      <CardContent>
        {error() ? (
          <Alert
            variant="danger"
            title="Sign out failed"
            description={error()}
            actions={<Button onPress={() => void signOutAndRedirect()}>Try again</Button>}
          />
        ) : (
          <div class="auth-status">
            <Spinner label="Signing out" />
            <p>Please wait while we clear your session.</p>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
