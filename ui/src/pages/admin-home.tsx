import { resource } from "@askrjs/askr/resources";
import { Badge } from "@askrjs/askr-ui/badge";
import { Button } from "@askrjs/askr-ui/button";
import { Container } from "@askrjs/askr-ui/container";
import { Stack } from "@askrjs/askr-ui/stack";
import { ArrowRight, Gauge, LogOut } from "@askrjs/icons-lucide";
import { deleteSession, fetchSession } from "../resources/session";

export default function AdminHome() {
  const session = resource(async () => fetchSession(), []);

  if (session.value === null && typeof window !== "undefined") {
    queueMicrotask(() => {
      window.location.replace("/login");
    });
  }

  async function onSignOut() {
    await deleteSession();
    window.location.replace("/login");
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
      <Container>
        <Stack gap="1.25rem">
          <div class="panel-heading">
            <Badge class="status-badge">Authenticated</Badge>
            <p class="eyebrow">Admin Home</p>
          </div>

          <div class="panel-copy">
            <h1>Welcome, {session.value.username}</h1>
            <p>
              The Fitz admin SPA is now mounted at the root path and ready for
              feature work. This baseline confirms the shell, auth flow, and API
              wiring are in place.
            </p>
          </div>

          <div class="stats-callout">
            <Gauge size={18} />
            <span>
              Live broker inspection stays behind the existing authenticated
              admin API.
            </span>
          </div>

          <div class="admin-actions">
            <Button asChild class="primary-action">
              <a href="/api/v1/stats">
                <ArrowRight size={16} />
                Open Stats API
              </a>
            </Button>
            <Button class="secondary-action" onPress={onSignOut}>
              <LogOut size={16} />
              Sign out
            </Button>
          </div>
        </Stack>
      </Container>
    </section>
  );
}
