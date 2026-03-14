import { resource } from "@askrjs/askr/resources";
import { deleteSession, fetchSession } from "../resources/session";

export default function AdminHome() {
  const session = resource(async () => fetchSession(), []);

  if (session.value === null && typeof window !== "undefined") {
    queueMicrotask(() => {
      window.location.replace("/admin/login");
    });
  }

  async function onSignOut() {
    await deleteSession();
    window.location.replace("/admin/login");
  }

  if (session.pending) {
    return (
      <section class="admin-panel">
        <p>Checking your admin session...</p>
      </section>
    );
  }

  if (session.error) {
    return (
      <section class="admin-panel">
        <h1>Admin session unavailable</h1>
        <p>We could not load your admin session right now.</p>
      </section>
    );
  }

  if (!session.value) {
    return (
      <section class="admin-panel">
        <p>Redirecting to sign in...</p>
      </section>
    );
  }

  return (
    <section class="admin-panel">
      <p class="eyebrow">Admin Home</p>
      <h1>Welcome, {session.value.username}</h1>
      <p>
        Your admin session is active. This is the landing page that future
        management views will build on.
      </p>
      <div class="admin-actions">
        <a href="/api/v1/stats" role="button" class="secondary">
          Open Stats API
        </a>
        <button onClick={onSignOut}>Sign out</button>
      </div>
    </section>
  );
}
