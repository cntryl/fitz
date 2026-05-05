import { Button } from "@askrjs/ui";
import { ArrowRightIcon, GaugeIcon, LogOutIcon } from "@askrjs/lucide";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { createSignOutMutation } from "@/features/session/session-mutation";

export default function AdminHome() {
  const session = createCurrentSessionQuery();
  const signOut = createSignOutMutation();

  if (!session.loading && session.data === null && typeof window !== "undefined") {
    queueMicrotask(() => {
      window.location.replace("/login");
    });
  }

  async function onSignOut() {
    await signOut.execute(undefined);
    window.location.replace("/login");
  }

  if (session.loading) {
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

  if (!session.data) {
    return (
      <section class="admin-panel">
        <p>Redirecting to sign in...</p>
      </section>
    );
  }

  return (
    <section class="admin-panel">
      <div class="panel-heading">
        <span class="status-badge">Authenticated</span>
        <p class="eyebrow">Admin Home</p>
      </div>

      <div class="panel-copy">
        <h1>Welcome, {session.data.username}</h1>
        <p>
          The Fitz admin SPA is now mounted at the root path and ready for feature work. This
          baseline confirms the shell, auth flow, and API wiring are in place.
        </p>
      </div>

      <div class="stats-callout">
        <GaugeIcon size={18} />
        <span>Live broker inspection stays behind the existing authenticated admin API.</span>
      </div>

      <div class="admin-actions">
        <Button class="primary-action" onPress={() => window.location.assign("/api/v1/stats")}>
          <ArrowRightIcon size={16} />
          Open Stats API
        </Button>
        <Button class="secondary-action" onPress={onSignOut}>
          <LogOutIcon size={16} />
          Sign out
        </Button>
      </div>
    </section>
  );
}
