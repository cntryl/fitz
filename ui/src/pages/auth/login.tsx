import { state } from "@askrjs/askr";
import { task, watch } from "@askrjs/askr/resources";
import { currentRoute, navigate } from "@askrjs/askr/router";
import {
  Alert,
  Block,
  Button,
  Field,
  Input,
  Label,
  PageHeader,
  Spinner,
} from "@askrjs/themes/components";
import AuthBrand from "@/components/shared/auth-brand";
import { createSignInMutation } from "@/features/session/session-mutation";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { manageRoutePageContext } from "@/components/shared/domain-page-frame";
import { formatUnknownError } from "@/shared/errors/format";

function resolveNextTarget(next: string | null) {
  if (!next?.startsWith("/") || next.startsWith("//") || next.includes("\\")) {
    return "/";
  }
  for (const character of next) {
    const code = character.charCodeAt(0);
    if (code < 32 || code === 127) return "/";
  }

  const target = new URL(next, window.location.origin);
  if (
    target.origin !== window.location.origin ||
    target.pathname.startsWith("//") ||
    target.pathname.startsWith("/api/") ||
    target.pathname === "/login" ||
    target.pathname === "/logout"
  ) {
    return "/";
  }
  return `${target.pathname}${target.search}${target.hash}`;
}

function openProvider(next: string) {
  window.location.replace(`/api/v1/session/oidc/start?${new URLSearchParams({ next })}`);
}

export default function Login() {
  const [username, setUsername] = state("");
  const [password, setPassword] = state("");

  task(() => manageRoutePageContext("Sign in"));

  const signIn = createSignInMutation();
  const currentSession = createCurrentSessionQuery();
  const { query } = currentRoute();
  const nextTarget = resolveNextTarget(query.get("next"));
  const callbackFailed = query.get("oidc_error") === "1";
  const signedOut = query.get("signed_out") === "1";
  const stopped = callbackFailed || signedOut;
  const openAccess = currentSession.data?.authRequired === false;

  watch(
    [
      () => currentSession.data?.oidcEnabled === true,
      () => currentSession.data?.authenticated === true,
      () => Boolean(currentSession.error),
      () => stopped,
      () => nextTarget,
    ] as const,
    ([enabled, authenticated, failed, paused, next]) => {
      if (!enabled || failed || paused) return;
      if (authenticated) {
        navigate(next, { history: "replace" });
      } else {
        openProvider(next);
      }
    },
  );

  if (currentSession.loading || currentSession.error || currentSession.data?.oidcEnabled) {
    const title = currentSession.error
      ? "Could not verify your session"
      : callbackFailed
        ? "Sign in failed"
        : signedOut
          ? "Signed out"
          : "Sign in to Fitz Admin";
    return (
      <Block direction="column" gap="xl">
        <Block direction="column" gap="md">
          <AuthBrand />
          <PageHeader title={title} />
        </Block>
        {currentSession.error ? (
          <Button onPress={() => void currentSession.refresh()}>Retry</Button>
        ) : currentSession.loading || !stopped ? (
          <Spinner
            label={currentSession.loading ? "Checking sign-in options" : "Opening sign-in provider"}
          />
        ) : (
          <Button variant="primary" width="full" onPress={() => openProvider(nextTarget)}>
            {callbackFailed ? "Try again" : "Sign in"}
          </Button>
        )}
      </Block>
    );
  }

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
    <Block direction="column" gap="xl">
      <Block direction="column" gap="md">
        <AuthBrand />
        <PageHeader
          title={openAccess ? "Open access" : "Sign in to Fitz Admin"}
          description={
            openAccess
              ? "Admin authentication is disabled for this Fitz instance."
              : "Sign in as root with the configured root password."
          }
        />
      </Block>

      <div>
        {openAccess ? (
          <Alert
            variant="info"
            title="No credentials required"
            description="There is no browser account session to create while admin access is open."
            actions={
              <Button onPress={() => navigate(nextTarget, { history: "replace" })}>
                Continue to Fitz Admin
              </Button>
            }
          />
        ) : (
          <Block as="form" direction="column" gap="xl" onSubmit={onSubmit}>
            <Field>
              <Label for="username-field">Username</Label>
              <Input
                id="username-field"
                name="username"
                autocomplete="username"
                required
                value={username()}
                onInput={(event: Event) => setUsername((event.target as HTMLInputElement).value)}
                placeholder="root"
              />
            </Field>

            <Field>
              <Label for="password-field">Password</Label>
              <Input
                id="password-field"
                type="password"
                name="password"
                autocomplete="current-password"
                required
                value={password()}
                onInput={(event: Event) => setPassword((event.target as HTMLInputElement).value)}
                placeholder="Enter your password"
              />
            </Field>

            <Button
              type="submit"
              variant="primary"
              width="full"
              aria-busy={signIn.pending}
              disabled={signIn.pending}
            >
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
          </Block>
        )}
      </div>
    </Block>
  );
}
