import React, {useEffect, useRef, useState} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import CodeBlock from '@theme/CodeBlock';
import ThemedImage from '@theme/ThemedImage';
import styles from './index.module.css';

// Heroicons (outline, 24px).
function Icon({d}: {d: string}) {
  return (
    <svg className={styles.icon} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true">
      <path strokeLinecap="round" strokeLinejoin="round" d={d} />
    </svg>
  );
}

const SHOTS = [
  {key: 'filesystem', label: 'Filesystem', caption: "An image's merged filesystem, layer by layer, with any file open beside it."},
  {key: 'filesystem-secrets', label: 'Secrets', caption: 'Keys and tokens found in any layer, including one a later layer deleted.'},
  {key: 'filesystem-diff', label: 'Diff', caption: 'A file compared with its version in the layer below.'},
  {key: 'vulnerabilities', label: 'Vulnerabilities', caption: 'A Trivy or Grype report, stored next to the image as an OCI referrer.'},
  {key: 'pull-history', label: 'Pull history', caption: 'Who pulled which image, and when.'},
];

// The views take turns. The active tab's progress bar is the clock: when its
// CSS animation ends, the next view comes up, so pausing the tour is pausing
// that animation (the button, hovering the screen, or reduced motion).
function Hero() {
  const [index, setIndex] = useState(0);
  const [playing, setPlaying] = useState(true);
  const tabs = useRef<HTMLDivElement>(null);
  const shot = SHOTS[index];

  // Where the tabs scroll sideways, on a phone, keep the active one in view.
  useEffect(() => {
    const strip = tabs.current;
    const tab = strip?.children[index] as HTMLElement | undefined;
    if (strip && tab) {
      strip.scrollTo({left: tab.offsetLeft - (strip.clientWidth - tab.clientWidth) / 2, behavior: 'smooth'});
    }
  }, [index]);

  return (
    <header className={styles.hero}>
      <div className={styles.wrap}>
        <img className={styles.mark} src="/img/logo.svg" alt="" width="48" height="48" />
        <h1 className={styles.title}>
          A container registry that’s
          <br className={styles.wideOnly} /> simple to run and a pleasure to use
        </h1>
        <p className={styles.lede}>
          Angos is an OCI registry you configure in one file and run without a database. Its web UI shows
          what is inside every image, layer by layer, with scan reports and pull history alongside.
        </p>
        <div className={styles.actions}>
          <Link className={styles.primary} to="/docs/tutorials/quickstart">
            Get started
          </Link>
          <Link className={styles.secondary} to="https://github.com/project-angos/angos">
            View on GitHub
          </Link>
        </div>

        <div className={`${styles.stage} ${playing ? '' : styles.paused}`}>
          <div className={styles.controls}>
            <div ref={tabs} className={styles.segmented} role="tablist">
              {SHOTS.map((s, i) => (
                <button
                  key={s.key}
                  role="tab"
                  aria-selected={i === index}
                  onClick={() => {
                    setIndex(i);
                    setPlaying(false);
                  }}
                  onAnimationEnd={() => setIndex((i + 1) % SHOTS.length)}>
                  {s.label}
                </button>
              ))}
            </div>
            <button
              className={styles.play}
              onClick={() => setPlaying(!playing)}
              aria-label={playing ? 'Pause the tour' : 'Play the tour'}>
              <svg viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                <path d={playing ? 'M6 5h4v14H6zM14 5h4v14h-4z' : 'M7 4.5v15l12-7.5z'} />
              </svg>
            </button>
          </div>
          <div className={styles.screen}>
            {SHOTS.map((s, i) => (
              <ThemedImage
                key={s.key}
                className={i === index ? styles.shown : undefined}
                aria-hidden={i !== index}
                alt={s.caption}
                sources={{light: `/docs/images/ui-${s.key}-light.png`, dark: `/docs/images/ui-${s.key}-dark.png`}}
              />
            ))}
          </div>
        </div>
        <p className={styles.caption}>{shot.caption}</p>
      </div>
    </header>
  );
}

const CARE = [
  {title: 'Servers are disposable', body: 'Start another instance on the same storage and it serves everything the last one did.'},
  {title: 'Scale by adding replicas', body: 'Replicas share the same bucket, with nothing to coordinate them.'},
  {title: 'Maintenance while it serves', body: 'Garbage collection, scrub and prune run beside the live registry.'},
  {title: 'Changes without restarts', body: 'The configuration and TLS certificates reload when their files change.'},
];

function Care() {
  return (
    <section className={styles.section}>
      <div className={`${styles.wrap} ${styles.split}`}>
        <div>
          <p className={styles.eyebrow}>Operations</p>
          <h2 className={styles.h2}>Little to look after</h2>
          <p className={styles.text}>
            There is no database or lock service to run beside Angos. Everything it knows lives in the
            storage that holds your images.
          </p>
          <Link className={styles.more} to="/docs/explanation/architecture">
            How it works →
          </Link>
        </div>
        <div className={styles.care}>
          {CARE.map((c) => (
            <div key={c.title}>
              <h3>{c.title}</h3>
              <p>{c.body}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

// Harbor's containers are those of its default docker-compose install; a
// dashed part is one you add only in the case it names.
const STACKS = [
  {
    name: 'Harbor',
    summary: '9 containers',
    parts: ['nginx', 'portal', 'core', 'jobservice', 'registry', 'registryctl', 'log', 'redis', 'postgresql'],
    stateful: ['redis', 'postgresql'],
    facts: ['PostgreSQL and Redis', 'Over an HA PostgreSQL and Redis', 'Online', 'Built in', 'Projects with RBAC roles'],
  },
  {
    name: 'Zot',
    summary: '1 binary',
    parts: ['zot'],
    extra: 'DynamoDB or Redis, once shared',
    facts: ['Embedded BoltDB; DynamoDB or Redis once shared', 'Repositories hashed across members', 'Online', 'Built in', 'Per-repository policies'],
  },
  {
    name: 'Distribution',
    summary: '1 binary',
    parts: ['registry'],
    extra: 'Auth proxy or token server',
    facts: ['None; Redis optional as a cache', 'Replicas on shared storage', 'Registry read-only or stopped', 'None', 'htpasswd, a token server or an auth proxy'],
  },
  {
    name: 'Angos',
    summary: '1 binary',
    parts: ['angos'],
    facts: ['None; Redis optional as a cache', 'Replicas on shared storage', 'Online', 'Built in, down to image files', 'CEL policies and webhooks'],
  },
];

const ROWS = ['Database', 'Scaling', 'Garbage collection', 'Web UI', 'Access control'];

const CYLINDER = 'M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375';

function Comparison() {
  return (
    <section className={`${styles.section} ${styles.tinted}`}>
      <div className={styles.wrap}>
        <div className={styles.center}>
          <p className={styles.eyebrow}>Comparison</p>
          <h2 className={styles.h2}>How it compares</h2>
          <p className={styles.text}>
            Harbor and Zot bring a UI and policies, with more to operate as you scale. Distribution needs
            almost nothing and leaves the rest to you. Angos keeps the small footprint and brings the rest.
          </p>
        </div>
        <div className={styles.cards}>
          {STACKS.map((s) => (
            <div key={s.name} className={`${styles.card} ${s.name === 'Angos' ? styles.ours : ''}`}>
              <div className={styles.cardHead}>
                <h3>{s.name}</h3>
                <span>{s.summary}</span>
              </div>
              <div className={styles.parts}>
                {s.parts.map((p) => (
                  <span key={p} className={styles.part}>
                    {s.stateful?.includes(p) && <Icon d={CYLINDER} />}
                    {p}
                  </span>
                ))}
                {s.extra && <span className={`${styles.part} ${styles.partShared}`}>{s.extra}</span>}
              </div>
              <dl className={styles.cardFacts}>
                {ROWS.map((row, i) => (
                  <div key={row}>
                    <dt>{row}</dt>
                    <dd>{s.facts[i]}</dd>
                  </div>
                ))}
              </dl>
            </div>
          ))}
        </div>
        <p className={styles.note}>
          Harbor offers multi-tenant projects and a long track record, Zot a broad set of extensions, and
          Distribution is the reference implementation others build on. Figures are from Harbor 2.15.2's
          default install and the Zot 2.1.21 and Distribution 3.1.1 documentation.
        </p>
      </div>
    </section>
  );
}

const FEATURES = [
  {title: 'OCI 1.1 compliant', to: '/docs/reference/api-endpoints', icon: 'm20.25 7.5-.625 10.632a2.25 2.25 0 0 1-2.247 2.118H6.622a2.25 2.25 0 0 1-2.247-2.118L3.75 7.5M10 11.25h4M3.375 7.5h17.25c.621 0 1.125-.504 1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125Z', body: 'Docker, Podman, containerd and any OCI tool, with referrers for signatures and SBOMs.'},
  {title: 'Pull-through cache', to: '/docs/explanation/pull-through-caching', icon: 'M7.5 21 3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5', body: 'Mirror Docker Hub, ghcr.io or any upstream; immutable tags skip the upstream check.'},
  {title: 'Replication', to: '/docs/explanation/replication', icon: 'M16.023 9.348h4.992V4.356m0 4.992-3.181-3.183a8.25 8.25 0 0 0-13.803 3.7M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7', body: 'Mirror repositories to downstream registries in both directions, with a durable retry queue.'},
  {title: 'Access policies', to: '/docs/how-to/set-up-access-control', icon: 'M16.5 10.5V6.75a4.5 4.5 0 1 0-9 0v3.75m-.75 11.25h10.5a2.25 2.25 0 0 0 2.25-2.25v-6.75a2.25 2.25 0 0 0-2.25-2.25H6.75a2.25 2.25 0 0 0-2.25 2.25v6.75a2.25 2.25 0 0 0 2.25 2.25Z', body: 'CEL expressions decide who may push, pull or delete, per repository.'},
  {title: 'Passwordless CI', to: '/docs/how-to/configure-github-actions-oidc', icon: 'M15.75 5.25a3 3 0 0 1 3 3m3 0a6 6 0 0 1-7.029 5.912c-.563-.097-1.159.026-1.563.43L10.5 17.25H8.25v2.25H6v2.25H2.25v-2.818c0-.597.237-1.17.659-1.591l6.499-6.499c.404-.404.527-1 .43-1.563A6 6 0 1 1 21.75 8.25Z', body: 'OIDC from GitHub Actions, Kubernetes or any issuer, and a token service.'},
  {title: 'Retention', to: '/docs/how-to/configure-retention-policies', icon: 'M12 6v6h4.5m4.5 0a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z', body: 'Keep tags by age, semver pattern, or recent pushes and pulls.'},
  {title: 'Vulnerability scanning', to: '/docs/how-to/scan-images', icon: 'M9 12.75 11.25 15 15 9.75m-3-7.036A11.959 11.959 0 0 1 3.598 6 11.99 11.99 0 0 0 3 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285Z', body: 'Trivy or Grype on push, the report kept next to the image.'},
  {title: 'Immutable tags', to: '/docs/how-to/protect-tags-immutability', icon: 'M9.568 3H5.25A2.25 2.25 0 0 0 3 5.25v4.318c0 .597.237 1.17.659 1.591l9.581 9.581c.699.699 1.78.872 2.607.33a18.095 18.095 0 0 0 5.223-5.223c.542-.827.369-1.908-.33-2.607L11.16 3.66A2.25 2.25 0 0 0 9.568 3ZM6 6h.008v.008H6V6Z', body: 'Protect release tags from being overwritten, with exclusions such as latest.'},
  {title: 'Web UI', to: '/docs/how-to/enable-web-ui', icon: 'M9 17.25v1.007a3 3 0 0 1-.879 2.122L7.5 21h9l-.621-.621A3 3 0 0 1 15 18.257V17.25m6-12V15a2.25 2.25 0 0 1-2.25 2.25H5.25A2.25 2.25 0 0 1 3 15V5.25m18 0A2.25 2.25 0 0 0 18.75 3H5.25A2.25 2.25 0 0 0 3 5.25m18 0V12a2.25 2.25 0 0 1-2.25 2.25H5.25A2.25 2.25 0 0 1 3 12V5.25', body: 'Browse repositories, manifests, referrers, pull history and image filesystems.'},
];

const EXTRAS = [
  {label: 'Mutual TLS', to: '/docs/how-to/configure-mtls'},
  {label: 'Webhook authorization', to: '/docs/how-to/configure-webhook-authorization'},
  {label: 'Event webhooks', to: '/docs/how-to/configure-event-webhooks'},
  {label: 'Prometheus metrics', to: '/docs/reference/metrics'},
  {label: 'Kubernetes', to: '/docs/how-to/deploy-kubernetes'},
];

function Features() {
  return (
    <section className={styles.section}>
      <div className={styles.wrap}>
        <div className={styles.center}>
          <p className={styles.eyebrow}>Features</p>
          <h2 className={styles.h2}>What’s included</h2>
        </div>
        <div className={styles.features}>
          {FEATURES.map((f) => (
            <Link key={f.title} to={f.to} className={styles.feature}>
              <Icon d={f.icon} />
              <h3>{f.title}</h3>
              <p>{f.body}</p>
            </Link>
          ))}
        </div>
        <p className={styles.extras}>
          Also{' '}
          {EXTRAS.map((e, i) => (
            <React.Fragment key={e.label}>
              {i > 0 && <span aria-hidden="true"> · </span>}
              <Link to={e.to}>{e.label}</Link>
            </React.Fragment>
          ))}
        </p>
      </div>
    </section>
  );
}

function Policy() {
  return (
    <section className={`${styles.section} ${styles.tinted}`}>
      <div className={`${styles.wrap} ${styles.split}`}>
        <div>
          <p className={styles.eyebrow}>Configuration</p>
          <h2 className={styles.h2}>Rules you can read</h2>
          <p className={styles.text}>
            Access and retention are CEL expressions in one TOML file, reloaded on save. Here only
            main-branch builds of your organization's repositories may push to production, and its twenty
            newest images are kept.
          </p>
          <Link className={styles.more} to="/docs/reference/cel-expressions">
            Expression reference →
          </Link>
        </div>
        <CodeBlock language="toml" title="config.toml">
{`[auth.oidc.github-actions]
issuer = "https://token.actions.githubusercontent.com"

[repository."production".access_policy]
default = "deny"
rules = ['''
  identity.oidc != null &&
  identity.oidc.claims["ref"] == "refs/heads/main" &&
  identity.oidc.claims["repository"].startsWith("myorg/")
''']

[repository."production".retention_policy]
rules = ['image.tag == "latest"', 'top_pushed(20)']`}
        </CodeBlock>
      </div>
    </section>
  );
}

function Start() {
  return (
    <section className={styles.section}>
      <div className={`${styles.wrap} ${styles.split}`}>
        <div>
          <p className={styles.eyebrow}>Get started</p>
          <h2 className={styles.h2}>Running in five minutes</h2>
          <ol className={styles.steps}>
            <li>Download the binary for your platform.</li>
            <li>Write a configuration naming a storage and a repository.</li>
            <li>Start the server and push an image.</li>
          </ol>
          <div className={styles.actionsLeft}>
            <Link className={styles.primary} to="/docs/tutorials/quickstart">
              Read the quickstart
            </Link>
            <Link className={styles.secondary} to="https://github.com/project-angos/angos/releases">
              Releases
            </Link>
          </div>
        </div>
        <CodeBlock language="bash" title="terminal">
{`curl -LO https://github.com/project-angos/angos/releases/latest/download/angos-linux-amd64
chmod +x angos-linux-amd64

cat > config.toml << 'EOF'
[server]
bind_address = "0.0.0.0"

[blob_store.fs]
root_dir = "./registry-data"

[global.access_policy]
default = "allow"

[repository."test"]
EOF

./angos-linux-amd64 -c config.toml server &
docker tag alpine:latest localhost:8000/test/alpine:latest
docker push localhost:8000/test/alpine:latest`}
        </CodeBlock>
      </div>
      <p className={`${styles.wrap} ${styles.etymology}`}>
        Nobody here speaks ancient Greek. Angos (<span lang="grc">ἄγγος</span>) just sounded good, and
        means vessel.
      </p>
    </section>
  );
}

export default function Home(): React.JSX.Element {
  return (
    <Layout description="An OCI container registry in a single Rust binary, with no database to run.">
      <Hero />
      <main>
        <Care />
        <Comparison />
        <Features />
        <Policy />
        <Start />
      </main>
    </Layout>
  );
}
