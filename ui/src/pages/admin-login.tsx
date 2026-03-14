import { state } from "@askrjs/askr";
import { resource } from "@askrjs/askr/resources";
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
      <div class="auth-intro">
        <p class="eyebrow">Admin Access</p>
        <h1>Sign in to Fitz Admin</h1>
        <p>
          Use your admin credentials to access the REST-backed management UI.
        </p>
      </div>

      <form class="auth-form" onSubmit={onSubmit}>
        <label>
          Username
          <input
            name="username"
            autocomplete="username"
            value={username()}
            onInput={(event: Event) =>
              username.set((event.target as HTMLInputElement).value)
            }
            placeholder="admin"
          />
        </label>

        <label>
          Password
          <input
            type="password"
            name="password"
            autocomplete="current-password"
            value={password()}
            onInput={(event: Event) =>
              password.set((event.target as HTMLInputElement).value)
            }
            placeholder="Enter your password"
          />
        </label>

        {error() ? <p class="auth-error">{error()}</p> : null}
        {session.error ? (
          <p class="auth-error">Unable to check your current session.</p>
        ) : null}

        <button type="submit" aria-busy={submitting()}>
          {submitting() ? "Signing in..." : "Sign in"}
        </button>
      </form>
    </section>
  );
}
