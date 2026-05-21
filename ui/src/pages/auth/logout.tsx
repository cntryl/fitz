import { state } from "@askrjs/askr";
import { task } from "@askrjs/askr/resources";
import { navigate } from "@askrjs/askr/router";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { createSignOutMutation } from "@/features/session/session-mutation";

export default function Logout() {
  const signOut = createSignOutMutation();
  const error = state("");

  task(async () => {
    try {
      error.set("");
      await signOut.execute(undefined);
      navigate("/login", { history: "replace" });
    } catch (err) {
      error.set(err instanceof Error ? err.message : "Unable to sign out");
    }
  });

  return (
    <Card class="auth-card" variant="raised">
      <CardHeader>
        <CardTitle>Signing out</CardTitle>
        <CardDescription>
          Ending your Fitz admin session and returning to login.
        </CardDescription>
      </CardHeader>
      <CardContent>
        {error() ? <p class="auth-error">{error()}</p> : <p>Please wait while we clear your session.</p>}
      </CardContent>
    </Card>
  );
}
