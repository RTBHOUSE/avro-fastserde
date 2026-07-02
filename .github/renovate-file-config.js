module.exports = {
    "autodiscover": true,
    "hostRules": [
        {
            hostType: 'github',
            matchHost: 'https://api.github.com/repos/rtbhouse-platform-engineering',
            token: process.env.RENOVATE_CONFIG_PRESET_TOKEN,
        },
    ],
};
