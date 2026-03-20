import { state } from "@askrjs/askr";
import { resource } from "@askrjs/askr/resources";
import { Badge } from "@askrjs/askr-ui/badge";
import { Button } from "@askrjs/askr-ui/button";
import {
  Field,
  FieldControl,
  FieldDescription,
  FieldError,
  FieldLabel,
} from "@askrjs/askr-ui/field";
import { Input } from "@askrjs/askr-ui/input";
import { Stack } from "@askrjs/askr-ui/stack";
import { LockKeyhole, ShieldCheck } from "@askrjs/icons-lucide";
import { createSession, fetchSession } from "../resources/session";

export default function AdminLogin() {
  const username = state("");
  const password = state("");
  const error = state("");
  const submitting = state(false);

  const session = resource(async () => fetchSession(), []);

  if (session.value?.authenticated && typeof window !== "undefined") {
    queueMicrotask(() => {
      window.location.replace("/admin");
    });
  }

  async function onSubmit(event: Event) {
    event.preventDefault();
    error.set("");
    submitting.set(true);

    try {
      await createSession({
        username: username(),
        password: password(),
      });
      window.location.replace("/admin");
    } catch (err) {
      error.set(err instanceof Error ? err.message : "Unable to sign in");
    } finally {
      submitting.set(false);
    }
  }

  return (
    <section class="auth-card">
      <Stack gap="1.5rem">
        <div class="auth-intro">
          <Badge class="shell-badge">Root SPA</Badge>
          <p class="eyebrow">Admin Access</p>
          <h1>Sign in to Fitz Admin</h1>
          <p>
            Use your admin credentials to access the REST-backed management UI.
          </p>
        </div>

        <div class="auth-points">
          <div class="auth-point">
            <ShieldCheck size={16} />
            <span>Session-backed admin authentication</span>
          </div>
          <div class="auth-point">
            <LockKeyhole size={16} />
            <span>Prepared for additional SPA routes and features</span>
          </div>
        </div>

        <form class="auth-form" onSubmit={onSubmit}>
          <Field class="auth-field" id="username-field">
            <FieldLabel fieldId="username-field">Username</FieldLabel>
            <FieldDescription fieldId="username-field">
              Use the Fitz admin account configured on the backend.
            </FieldDescription>
            <FieldControl asChild fieldId="username-field">
              <Input
                name="username"
                autocomplete="username"
                value={username()}
                onInput={(event: Event) =>
                  username.set((event.target as HTMLInputElement).value)
                }
                placeholder="admin"
              />
            </FieldControl>
          </Field>

          <Field class="auth-field" id="password-field">
            <FieldLabel fieldId="password-field">Password</FieldLabel>
            <FieldDescription fieldId="password-field">
              Your current admin password is sent to the existing session API.
            </FieldDescription>
            <FieldControl asChild fieldId="password-field">
              <Input
                type="password"
                name="password"
                autocomplete="current-password"
                value={password()}
                onInput={(event: Event) =>
                  password.set((event.target as HTMLInputElement).value)
                }
                placeholder="Enter your password"
              />
            </FieldControl>
          </Field>

          {error() ? (
            <FieldError fieldId="password-field" class="auth-error">
              {error()}
            </FieldError>
          ) : null}
          {session.error ? (
            <p class="auth-error">Unable to check your current session.</p>
          ) : null}

          <Button type="submit" class="submit-action" aria-busy={submitting()}>
            {submitting() ? "Signing in..." : "Sign in"}
          </Button>
        </form>
      </Stack>
    </section>
  );
}
