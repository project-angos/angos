---
displayed_sidebar: tutorials
sidebar_position: 2
title: "Your First Private Registry"
---

# Your First Private Registry

Set up a registry with user authentication and access control policies.

## What You'll Learn

By the end of this tutorial, you will:
- Create a user with password authentication
- Configure an access control policy
- Test authenticated push and pull operations

## Prerequisites

- Completed the [Quickstart](quickstart.md) tutorial
- Angos binary available

## Step 1: Generate a Password Hash

Angos uses Argon2 for secure password hashing. Generate a hash for your user:

```bash
./angos argon
```

When prompted, enter a password. The tool outputs an Argon2 hash:
```
Input Password:
$argon2id$v=19$m=19456,t=2,p=1$...
```

Copy this hash for the next step.

## Step 2: Configure Authentication

Update your `config.toml` to add a user identity:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.fs]
root_dir = "./registry-data"

[repository."test"]

[auth.identity.alice]
username = "alice"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."  # Paste your hash here
```

## Step 3: Add an Access Control Policy

Add a global access policy that requires authentication:

```toml
[global.access_policy]
default = "deny"
rules = [
  "identity.username != null"
]
```

Your complete `config.toml` should now look like:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.fs]
root_dir = "./registry-data"

[auth.identity.alice]
username = "alice"
password = "$argon2id$v=19$m=19456,t=2,p=1$..."

[global.access_policy]
default = "deny"
rules = [
  "identity.username != null"
]

[repository."test"]
```

## Step 4: Configure Docker for Insecure Registry

Since this tutorial uses HTTP (no TLS), configure Docker to allow insecure connections.

Edit or create `/etc/docker/daemon.json`:

```json
{
  "insecure-registries": ["localhost:8000"]
}
```

Then restart Docker:

```bash
# Linux
sudo systemctl restart docker

# macOS (Docker Desktop)
# Restart Docker Desktop from the menu bar
```

## Step 5: Start the Registry

```bash
./angos -c config.toml server
```

## Step 6: Test Anonymous Access (Should Fail)

Try to push without authentication:

```bash
docker tag alpine:latest localhost:8000/test/alpine:latest
docker push localhost:8000/test/alpine:latest
```

You should see an authentication error:
```
unauthorized: Access denied
```

## Step 7: Login and Push

Login with your credentials:

```bash
docker login localhost:8000 -u alice
```

Enter your password when prompted. Now push the image:

```bash
docker push localhost:8000/test/alpine:latest
```

The push should succeed.

## Step 8: Verify the Configuration

You can also verify using curl:

```bash
# Without auth (should fail)
curl http://localhost:8000/v2/

# With auth (should succeed)
curl -u alice:yourpassword http://localhost:8000/v2/
```

## Understanding the Policy

The access policy uses CEL (Common Expression Language):

```toml
rules = [
  "identity.username != null"
]
```

This rule allows access when `identity.username` is not empty, meaning the user has authenticated with valid credentials.

## What's Next?

Now that you have a private registry, you can:

- **Add more users**: Create additional `[auth.identity.<name>]` sections
- **Fine-grained policies**: See [How to Set Up Access Control](../how-to/set-up-access-control.md) for repository-specific rules
- **Use OIDC**: See [Configure GitHub Actions OIDC](../how-to/configure-github-actions-oidc.md) for token-based authentication
- **Enable TLS**: See [Configure mTLS](../how-to/configure-mtls.md) for encrypted connections

## Reference

- [CEL Expressions Reference](../reference/cel-expressions.md) - All available variables and functions
- [Configuration Reference](../reference/configuration.md) - Complete configuration options
