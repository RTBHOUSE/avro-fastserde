const extraLookupToken = process.env.RENOVATE_EXTRA_LOOKUP_TOKEN;
const extraLookupOwner = (process.env.RENOVATE_EXTRA_LOOKUP_OWNER || '').trim();

// RENOVATE_EXTRA_LOOKUP_REPOSITORIES is newline-separated; be liberal about
// whitespace/commas, and tolerate "owner/repo" as well as a bare "repo".
const extraLookupRepositories = (process.env.RENOVATE_EXTRA_LOOKUP_REPOSITORIES || '')
    .split(/[\s,]+/)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => entry.split('/').filter(Boolean).pop())
    .filter(Boolean);

const extraLookupHostRules = [];

if (extraLookupToken && extraLookupOwner && extraLookupRepositories.length > 0) {
    for (const repository of extraLookupRepositories) {
        const slug = `${extraLookupOwner}/${repository}`;
        // Renovate matches URL-form matchHost with a case-sensitive startsWith(),
        // and the request URL case comes from how the consumer wrote the dependency
        // reference. Emit both the canonical-case and the lower-case variant.
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
    "hostRules": [
        {
            hostType: 'github',
            matchHost: 'https://api.github.com/repos/rtbhouse-platform-engineering/renovate-scanner',
            token: process.env.RENOVATE_CONFIG_PRESET_TOKEN,
        },
        ...extraLookupHostRules,
    ],
};
