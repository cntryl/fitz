import { state } from "@askrjs/askr";
import { currentRoute } from "@askrjs/askr/router";
import { Button, Input, Label } from "@askrjs/ui";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@askrjs/themes/components";
import { createSignInMutation } from "@/features/session/session-mutation";

function resolveNextTarget() {
  const next = currentRoute().query.get("next");

  if (typeof next === "string" && next.startsWith("/") && !next.startsWith("//")) {
    return next;
  }

  return "/admin";
}

export default function AdminLogin() {
  const username = state("");
  const password = state("");
  const error = state("");
  const submitting = state(false);

  const signIn = createSignInMutation();
  const nextTarget = resolveNextTarget();

  async function onSubmit(event: Event) {
    event.preventDefault();
    error.set("");
    submitting.set(true);

    try {
      await signIn.execute({
        username: username(),
        password: password(),
      });
      if (typeof window !== "undefined") {
        window.location.replace(nextTarget);
      }
    } catch (err) {
      error.set(err instanceof Error ? err.message : "Unable to sign in");
    } finally {
      submitting.set(false);
    }
  }

  return (
    <Card class="auth-card" variant="raised">
      <CardHeader>
          <CardTitle>Sign in to Fitz Admin</CardTitle>
          <CardDescription>
            Use your admin credentials to access the REST-backed management UI.
          </CardDescription>
      </CardHeader>

      <CardContent>
        <form class="auth-form" onSubmit={onSubmit}>
          <div class="auth-field">
            <Label for="username-field">Username</Label>
            <Input
              id="username-field"
              name="username"
              autocomplete="username"
              value={username()}
              onInput={(event: Event) => username.set((event.target as HTMLInputElement).value)}
              placeholder="admin"
            />
          </div>

          <div class="auth-field">
            <Label for="password-field">Password</Label>
            <Input
              id="password-field"
              type="password"
              name="password"
              autocomplete="current-password"
              value={password()}
              onInput={(event: Event) => password.set((event.target as HTMLInputElement).value)}
              placeholder="Enter your password"
            />
          </div>

          {error() ? <p class="auth-error">{error()}</p> : null}

          <Button type="submit" aria-busy={submitting()}>
            {submitting() ? "Signing in..." : "Sign in"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}
