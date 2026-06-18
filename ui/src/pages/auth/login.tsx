import { state } from "@askrjs/askr";
import { currentRoute, navigate } from "@askrjs/askr/router";
import { Input, Label } from "@askrjs/ui";
import { Button, Field, FieldHint } from "@askrjs/themes/controls";
import {
  Alert,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/surfaces";
import { createSignInMutation } from "@/features/session/session-mutation";
import { formatUnknownError } from "@/shared/errors/format";

const usernameHintId = "login-username-hint";
const passwordHintId = "login-password-hint";

function resolveNextTarget() {
  const next = currentRoute().query.get("next");

  if (typeof next === "string" && next.startsWith("/") && !next.startsWith("//")) {
    return next;
  }

  return "/";
}

export default function Login() {
  const [username, setUsername] = state("");
  const [password, setPassword] = state("");

  const signIn = createSignInMutation();
  const nextTarget = resolveNextTarget();

  async function onSubmit(event: Event) {
    event.preventDefault();

    signIn.reset();

    try {
      await signIn.execute({
        username: username(),
        password: password(),
      });
      navigate(nextTarget, { history: "replace" });
    } catch {
      return;
    }
  }

  return (
    <Card class="auth-card" variant="raised">
      <CardHeader>
        <CardTitle>Sign in to Fitz Admin</CardTitle>
        <CardDescription>Use your Fitz Admin account to continue.</CardDescription>
      </CardHeader>

      <CardContent>
        <form class="auth-form" onSubmit={onSubmit}>
          <Field>
            <Label for="username-field">Username</Label>
            <Input
              id="username-field"
              name="username"
              autocomplete="username"
              required
              aria-describedby={usernameHintId}
              value={username()}
              onInput={(event: Event) => setUsername((event.target as HTMLInputElement).value)}
              placeholder="admin"
            />
            <FieldHint id={usernameHintId}>Use the Fitz Admin username for this workspace.</FieldHint>
          </Field>

          <Field>
            <Label for="password-field">Password</Label>
            <Input
              id="password-field"
              type="password"
              name="password"
              autocomplete="current-password"
              required
              aria-describedby={passwordHintId}
              value={password()}
              onInput={(event: Event) => setPassword((event.target as HTMLInputElement).value)}
              placeholder="Enter your password"
            />
            <FieldHint id={passwordHintId}>Use the current password for that account.</FieldHint>
          </Field>

          <Button class="auth-submit" type="submit" variant="primary" aria-busy={signIn.pending} disabled={signIn.pending}>
            {signIn.pending ? "Signing in..." : "Sign in"}
          </Button>

          <div class="auth-status-shell" aria-live="assertive" aria-atomic="true">
            {signIn.error ? (
              <Alert
                variant="danger"
                title="Sign in failed"
                description={formatUnknownError(signIn.error)}
              />
            ) : null}
          </div>
        </form>
      </CardContent>
    </Card>
  );
}
