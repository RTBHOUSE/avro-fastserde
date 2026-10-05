const extraLookupToken = process.env.RENOVATE_EXTRA_LOOKUP_TOKEN;
const extraLookupOwner = (process.env.RENOVATE_EXTRA_LOOKUP_OWNER || '').trim();

const extraLookupRepositories = (process.env.RENOVATE_EXTRA_LOOKUP_REPOSITORIES || '')
    .split(/[\s,]+/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => entry.split('/').filter(Boolean).pop())
    .filter(Boolean);

const extraLookupHostRules = [];

// Cloud-agnostic passthrough for org-specific credentials (e.g. a short-lived
// GCP Artifact Registry token minted by the caller workflow). The workflow
// builds this as a JSON object and passes it through run-renovate's
// `renovate-secrets` input; orgs with no such step pass "" and get {}.
// Renovate's own `{{ secrets.X }}` config templating (in each org's preset
// chain) resolves against this at runtime.
let secrets = {};
const rawSecrets = process.env.RENOVATE_SECRETS;
if (rawSecrets) {
    try {
        secrets = JSON.parse(rawSecrets);
    } catch (err) {
        throw new Error(`RENOVATE_SECRETS is set but not valid JSON: ${err.message}`);
    }
}

if (extraLookupToken && extraLookupOwner && extraLookupRepositories.length > 0) {
    for (const repository of extraLookupRepositories) {
        const slug = `${extraLookupOwner}/${repository}`;
        for (const variant of new Set([slug, slug.toLowerCase()])) {
            extraLookupHostRules.push({
                hostType: 'github',
                matchHost: `https://api.github.com/repos/${variant}`,
                token: extraLookupToken,
            });
        }
    }
}

module.exports = {
    "autodiscover": true,
    // Allowlist for postUpgradeTasks commands (renovate/committed-dist.json5).
    // Renovate rejects any post-upgrade command not matching these patterns,
    // so target repos cannot execute anything else via their renovate.json5.
    "allowedCommands": [
        "^npm ci$",
        "^npm run build$",
    ],
    "hostRules": [
        {
            hostType: 'github',
            matchHost: 'https://api.github.com/repos/rtbhouse-platform-engineering/renovate-scanner',
            token: process.env.RENOVATE_CONFIG_PRESET_TOKEN,
        },
        ...extraLookupHostRules,
    ],
    "secrets": secrets,
};
