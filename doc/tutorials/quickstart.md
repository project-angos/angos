---
displayed_sidebar: tutorials
sidebar_position: 1
title: "Quickstart"
---

# Quickstart: Your First Container Registry

Get Angos running and push your first container image in under 5 minutes.

## What You'll Learn

By the end of this tutorial, you will:
- Run a Angos server
- Push a container image to your registry
- Pull the image back

## Prerequisites

- Docker installed and running
- A terminal

## Step 1: Download Angos

Download the latest release for your platform from the [releases page](https://github.com/project-angos/angos/releases):

```bash
# Example for Linux x86_64
curl -LO https://github.com/project-angos/angos/releases/latest/download/angos-linux-amd64
chmod +x angos-linux-amd64
mv angos-linux-amd64 angos
```

Or build from source:

```bash
git clone https://github.com/project-angos/angos.git
cd angos
cargo build --release
cp target/release/angos .
```

## Step 2: Create a Minimal Configuration

Create a file named `config.toml`:

```toml
[server]
bind_address = "0.0.0.0"
port = 8000

[blob_store.fs]
root_dir = "./registry-data"

[global.access_policy]
default = "allow"

[repository."test"]
```

This configures the registry to:
- Listen on all interfaces on port 8000 (the default port)
- Store images in the `./registry-data` directory
- Allow unauthenticated access (for testing only)
- Create a repository named `test` for pushing images

## Step 3: Start the Registry

```bash
./angos -c config.toml server
```

You should see output indicating the server is running:
```
Listening on 0.0.0.0:8000 (non-TLS)
```

## Step 4: Configure Docker for Insecure Registry

Since this quickstart uses HTTP (no TLS), you need to configure Docker to allow insecure connections.

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

> **Tip:** For production deployments, always use TLS. See [Deploy with Docker Compose](../how-to/deploy-docker-compose.md) for a secure setup.

## Step 5: Tag and Push an Image

In a new terminal, tag an existing image for your registry:

```bash
docker pull alpine:latest
docker tag alpine:latest localhost:8000/test/alpine:latest
```

Push the image to your registry:

```bash
docker push localhost:8000/test/alpine:latest
```

## Step 6: Pull the Image Back

Verify the image is stored by pulling it:

```bash
docker rmi localhost:8000/test/alpine:latest
docker pull localhost:8000/test/alpine:latest
```

You've successfully pushed and pulled an image from your own container registry.

## What's Next?

Now that you have a basic registry running, you can:

- **Add authentication**: See [Your First Private Registry](your-first-private-registry.md) to add user authentication and access control
- **Mirror Docker Hub**: See [Mirror Docker Hub](mirror-docker-hub.md) to set up a pull-through cache
- **Enable the Web UI**: See [How to Enable the Web UI](../how-to/enable-web-ui.md) for a visual interface
- **Deploy to production**: See [Deploy with Docker Compose](../how-to/deploy-docker-compose.md) or [Deploy on Kubernetes](../how-to/deploy-kubernetes.md)

## Configuration Reference

For all available configuration options, see the [Configuration Reference](../reference/configuration.md).
