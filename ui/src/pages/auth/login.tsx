import { state } from "@askrjs/askr";
import { task } from "@askrjs/askr/resources";
import { currentRoute, navigate } from "@askrjs/askr/router";
import {
  Alert,
  Block,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Field,
  Input,
  Label,
} from "@askrjs/themes/components";
import AuthBrand from "@/components/shared/auth-brand";
import { createSignInMutation } from "@/features/session/session-mutation";
import { createCurrentSessionQuery } from "@/features/session/session-query";
import { manageRoutePageContext } from "@/components/shared/domain-page-frame";
import { formatUnknownError } from "@/shared/errors/format";

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

  task(() => manageRoutePageContext("Sign in"));

  const signIn = createSignInMutation();
  const currentSession = createCurrentSessionQuery();
  const nextTarget = resolveNextTarget();
  const openAccess = currentSession.data?.authRequired === false;

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
    <Card variant="raised">
      <CardHeader>
        <AuthBrand />
        <CardTitle titleAs="h1">{openAccess ? "Open access" : "Sign in to Fitz Admin"}</CardTitle>
        <CardDescription>
          {openAccess
            ? "Admin authentication is disabled for this Fitz instance."
            : "Sign in as root with the configured root password."}
        </CardDescription>
      </CardHeader>

      <CardContent>
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
      </CardContent>
    </Card>
  );
}
