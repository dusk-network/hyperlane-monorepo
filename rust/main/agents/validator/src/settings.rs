//! Validator configuration.
//!
//! The correct settings shape is defined in the TypeScript SDK metadata. While the exact shape
//! and validations it defines are not applied here, we should mirror them.
//! ANY CHANGES HERE NEED TO BE REFLECTED IN THE TYPESCRIPT SDK.

use std::{collections::HashSet, ops::Add, path::PathBuf, time::Duration};

use aws_config::Region;
use derive_more::{AsMut, AsRef, Deref, DerefMut};
use eyre::{eyre, Context};
use hyperlane_base::{
    impl_loadable_from_settings,
    settings::{
        parser::{RawAgentConf, RawAgentSignerConf, ValueParser},
        CheckpointSyncerConf, Settings, SignerConf,
    },
};
use hyperlane_core::{
    cfg_unwrap_all, config::*, HyperlaneDomain, HyperlaneDomainProtocol, ReorgPeriod,
};
use itertools::Itertools;
use redact::Secret;
use serde::Deserialize;
use serde_json::Value;

use crate::checkpoint_consensus::CheckpointConsensus;

const DEFAULT_MAX_SIGN_CONCURRENCY: usize = 50;
// Bounds per-batch allocation and in-flight signing work while leaving ample
// headroom above the operational default. Keep in sync with the SDK schema.
const MAX_SIGN_CONCURRENCY: usize = 1_000;

/// Settings for RPCs
#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub url: Secret<String>,
    pub public: bool,
}

/// Settings for `Validator`
#[derive(Debug, AsRef, AsMut, Deref, DerefMut, Clone)]
pub struct ValidatorSettings {
    #[as_ref]
    #[as_mut]
    #[deref]
    #[deref_mut]
    base: Settings,

    /// Database path
    pub db: PathBuf,
    /// Chain to validate messages on
    pub origin_chain: HyperlaneDomain,
    /// The validator attestation signer
    pub validator: SignerConf,
    /// The checkpoint syncer configuration
    pub checkpoint_syncer: CheckpointSyncerConf,
    /// The reorg configuration
    pub reorg_period: ReorgPeriod,
    /// RPC-only DB polling and failed-verification retry interval, in fractional seconds.
    /// WebSocket insertions wake verification immediately. Defaults to 2s, overridable
    /// via `interval`, or via `chains.<originChainName>.index.interval` if
    /// `interval` is unset.
    pub interval: Duration,
    /// Live Merkle tree insertion source. Leaves are verified
    /// against on-chain checkpoints before signing, without per-leaf RPC log reads.
    /// RPC indexes history and resumes on stream failure or checkpoint mismatch.
    pub websocket_url: Option<Secret<url::Url>>,
    /// Root verification policy; single/fallback retain the classic signing path.
    pub(crate) checkpoint_consensus: Option<CheckpointConsensus>,
    /// A list of RPCs that the validator uses
    pub rpcs: Vec<RpcConfig>,
    /// If the validator oped into public RPCs
    pub allow_public_rpcs: bool,
    /// Test-only: skips on-chain self-announce. Never use in production.
    pub skip_announce: bool,
    /// Max sign concurrency
    pub max_sign_concurrency: usize,
}

#[derive(Debug, Deserialize)]
#[serde(transparent)]
struct RawValidatorSettings(Secret<Value>);

impl_loadable_from_settings!(Validator, RawValidatorSettings -> ValidatorSettings);

impl FromRawConf<RawValidatorSettings> for ValidatorSettings {
    /// Parse validator settings, preserving explicit interval, chain interval, then default precedence.
    fn from_config_filtered(
        raw: RawValidatorSettings,
        cwp: &ConfigPath,
        _filter: (),
        agent_name: &str,
    ) -> ConfigResult<Self> {
        let raw = raw.0.expose_secret().clone();
        let curr_dir = std::env::current_dir().map_err(|err| {
            let mut config_err = ConfigParsingError::default();
            config_err.push(cwp.clone(), eyre::eyre!(err.to_string()));
            config_err
        })?;

        let mut err = ConfigParsingError::default();

        let p = ValueParser::new(cwp.clone(), &raw);
        for removed in ["lightweight", "leightweigt"] {
            if raw.get(removed).is_some() {
                err.push(cwp.add(removed), eyre!(
                    "{removed} was removed; validators now require RPC indexing for startup and WebSocket recovery. Remove the setting."
                ));
            }
        }

        let origin_chain_name = p
            .chain(&mut err)
            .get_key("originChainName")
            .parse_string()
            .end();

        let allow_public_rpcs = p
            .chain(&mut err)
            .get_opt_key("allowPublicRpcs")
            .parse_bool()
            .unwrap_or(false);

        let skip_announce = p
            .chain(&mut err)
            .get_opt_key("skipAnnounce")
            .parse_bool()
            .unwrap_or(false);

        let origin_chain_name_set = origin_chain_name.map(|s| HashSet::from([s]));

        let consensus_name = origin_chain_name
            .and_then(|name| {
                p.chain(&mut err)
                    .get_key("chains")
                    .get_key(name)
                    .get_opt_key("rpcConsensusType")
                    .parse_string()
                    .end()
            })
            .unwrap_or("majority");
        let configured_consensus = match consensus_name {
            "quorum" => Some(CheckpointConsensus::Quorum),
            "majority" => Some(CheckpointConsensus::Majority),
            "single" | "fallback" => None,
            _ => {
                err.push(
                    cwp.clone(),
                    eyre!("Unknown rpcConsensusType: {consensus_name}"),
                );
                None
            }
        };
        let checkpoint_consensus = configured_consensus;
        // Quorum/majority vote on checkpoint history in the validator, not on raw
        // JSON responses. Indexing and auxiliary reads use ordinary fallback.
        // Normalize only the base parser's copy, preserving the configured endpoint metadata.
        // Dusk has no provider failover: keep its primary indexing/submission
        // connection explicit while retaining every endpoint in the voting pool.
        let mut base_raw = raw.clone();
        if configured_consensus.is_some() {
            if let Some(name) = origin_chain_name {
                if let Some(chain) = base_raw
                    .get_mut("chains")
                    .and_then(|chains| chains.get_mut(name.to_ascii_lowercase()))
                    .and_then(Value::as_object_mut)
                {
                    let dusk = chain.get("protocol").and_then(Value::as_str) == Some("dusk");
                    let base_consensus = if dusk { "single" } else { "fallback" };
                    chain.insert("rpcconsensustype".into(), base_consensus.into());
                }
            }
        }
        let base_parser = ValueParser::new(cwp.clone(), &base_raw);
        let base: Option<Settings> = base_parser
            .parse_from_raw_config::<Settings, RawAgentConf, Option<&HashSet<&str>>>(
                origin_chain_name_set.as_ref(),
                "Expected valid base agent configuration",
                agent_name.to_string(),
            )
            .take_config_err(&mut err);

        let origin_chain = if let (Some(base), Some(origin_chain_name)) = (&base, origin_chain_name)
        {
            base.lookup_domain(origin_chain_name)
                .context("Missing configuration for the origin chain")
                .take_err(&mut err, || cwp.add("origin_chain_name"))
        } else {
            None
        };

        let validator = p
            .chain(&mut err)
            .get_key("validator")
            .parse_from_raw_config::<SignerConf, RawAgentSignerConf, NoFilter>(
                (),
                "Expected valid validator configuration",
                agent_name.to_string(),
            )
            .end();

        let db = p
            .chain(&mut err)
            .get_opt_key("db")
            .parse_from_str("Expected db file path")
            .unwrap_or_else(|| {
                curr_dir.join(format!("validator_db_{}", origin_chain_name.unwrap_or("")))
            });

        let checkpoint_syncer = p
            .chain(&mut err)
            .get_key("checkpointSyncer")
            .and_then(parse_checkpoint_syncer)
            .end();

        cfg_unwrap_all!(cwp, err: [origin_chain_name]);

        let reorg_period = p
            .chain(&mut err)
            .get_key("chains")
            .get_key(origin_chain_name)
            .get_opt_key("blocks")
            .get_opt_key("reorgPeriod")
            .parse_value("Invalid reorgPeriod")
            .unwrap_or(ReorgPeriod::from_blocks(1));

        // Retains the 2s fallback #8843 established: only the precedence is new (explicit
        // `interval` -> chain's `index.interval` -> this default), not the default itself, to
        // avoid widening checkpoint-availability latency for validators that don't configure
        // either.
        const DEFAULT_INTERVAL: Duration = Duration::from_secs(2);
        let explicit_interval = p
            .chain(&mut err)
            .get_opt_key("interval")
            .parse_poll_interval_seconds()
            .end();
        let chain_interval = p
            .chain(&mut err)
            .get_key("chains")
            .get_key(origin_chain_name)
            .get_opt_key("index")
            .get_opt_key("interval")
            .parse_poll_interval_seconds()
            .end();
        let interval = explicit_interval
            .or(chain_interval)
            .unwrap_or(DEFAULT_INTERVAL);

        let chain = p
            .chain(&mut err)
            .get_key("chains")
            .get_key(origin_chain_name)
            .end()
            .ok_or_else(|| {
                let mut config_err = ConfigParsingError::default();
                config_err.push(cwp.clone(), eyre::eyre!("chains missing".to_string()));
                config_err
            })?;

        let configured_max_sign_concurrency = p
            .chain(&mut err)
            .get_opt_key("maxSignConcurrency")
            .parse_u64()
            .end();
        let max_sign_concurrency =
            parse_max_sign_concurrency(configured_max_sign_concurrency, cwp, &mut err);

        let websocket_url: Option<url::Url> = p
            .chain(&mut err)
            .get_opt_key("websocketUrl")
            .parse_from_str("Expected a valid Merkle tree hook WebSocket URL")
            .end();
        if let Some(url) = &websocket_url {
            if !matches!(url.scheme(), "ws" | "wss") {
                err.push(
                    cwp.clone(),
                    eyre::eyre!("`websocketUrl` must use ws:// or wss://"),
                );
            }
            if url.path() != "/validators" || url.query().is_some() {
                err.push(
                    cwp.clone(),
                    eyre::eyre!("`websocketUrl` must use /validators without query parameters"),
                );
            }
        }

        let mut rpcs = get_rpc_urls(&chain, "rpcUrls", "customRpcUrls", &mut err);
        // this is only relevant for cosmos
        rpcs.extend(get_rpc_urls(&chain, "grpcUrls", "customGrpcUrls", &mut err));
        // tron wallet urls
        rpcs.extend(get_rpc_urls(
            &chain,
            "walletUrls",
            "customWalletUrls",
            &mut err,
        ));
        rpcs.extend(get_rpc_urls(
            &chain,
            "walletSolidityUrls",
            "customWalletSolidityUrls",
            &mut err,
        ));

        for removed in ["additionalQuorumRpcUrls", "customAdditionalQuorumRpcUrls"] {
            if chain.chain(&mut err).get_opt_key(removed).end().is_some() {
                err.push(cwp.add("chains").add(origin_chain_name).add(&removed.to_ascii_lowercase()), eyre!(
                    "{removed} was removed; move its endpoints into rpcUrls/customRpcUrls and remove the obsolete setting. Validators use rpcConsensusType"
                ));
            }
        }

        cfg_unwrap_all!(cwp, err: [base, origin_chain, validator, checkpoint_syncer]);

        let mut base: Settings = base;
        // Tron and Ethereum both use secp256k1 keys, so the validator attestation
        // signer can double as the origin chain signer (used for self-announce txs).
        if matches!(
            origin_chain.domain_protocol(),
            HyperlaneDomainProtocol::Ethereum | HyperlaneDomainProtocol::Tron
        ) {
            if let Some(origin) = base.chains.get_mut(&origin_chain) {
                origin.signer.get_or_insert_with(|| validator.clone());
            }
        }

        err.into_result(Self {
            base,
            db,
            origin_chain,
            validator,
            checkpoint_syncer,
            reorg_period,
            interval,
            websocket_url: websocket_url.map(Secret::new),
            checkpoint_consensus,
            rpcs,
            allow_public_rpcs,
            skip_announce,
            max_sign_concurrency,
        })
    }
}

fn parse_max_sign_concurrency(
    configured: Option<u64>,
    cwp: &ConfigPath,
    err: &mut ConfigParsingError,
) -> usize {
    let configured = configured.unwrap_or(50);
    match usize::try_from(configured) {
        Ok(value @ 1..=MAX_SIGN_CONCURRENCY) => value,
        _ => {
            err.push(
                cwp.add("max_sign_concurrency"),
                eyre::eyre!("`maxSignConcurrency` must be between 1 and {MAX_SIGN_CONCURRENCY}"),
            );
            DEFAULT_MAX_SIGN_CONCURRENCY
        }
    }
}

/// Extracts all of the rpc urls
///
/// rpcKey is either grpcUrls or rpcUrls
/// overrideKey is either customGrpcUrls or customRpcUrls
fn get_rpc_urls(
    chain: &ValueParser,
    rpc_key: &str,
    override_key: &str,
    err: &mut ConfigParsingError,
) -> Vec<RpcConfig> {
    // struct looks like the following
    // ```rust
    // {
    //   rpc: [
    //     {
    //       "http": "http://my-rpc-url.com",
    //       "public": true
    //     }
    //   ]
    // }
    // ```
    let base = chain
        .chain(err)
        .get_opt_key(rpc_key)
        .into_array_iter()
        .map(|urls| {
            urls.filter_map(|v| {
                let public = v
                    .chain(err)
                    .get_opt_key("public")
                    .parse_bool()
                    .unwrap_or(false);
                let url: Option<&str> = v.chain(err).get_key("http").parse_string().end();
                url.map(|url| RpcConfig {
                    url: Secret::new(url.to_owned()),
                    public,
                })
            })
            .collect_vec()
        })
        .unwrap_or_default();
    let overrides = chain
        .chain(err)
        .get_opt_key(override_key)
        .parse_string()
        .end()
        .map(|urls| {
            urls.split(',')
                .map(str::trim)
                .filter(|url| !url.is_empty())
                .map(|url| RpcConfig {
                    url: Secret::new(url.to_owned()),
                    public: false,
                })
                .collect_vec()
        });
    overrides.unwrap_or(base)
}

/// Expects ValidatorAgentConfig.checkpointSyncer
fn parse_checkpoint_syncer(syncer: ValueParser) -> ConfigResult<CheckpointSyncerConf> {
    let mut err = ConfigParsingError::default();
    let syncer_type = syncer.chain(&mut err).get_key("type").parse_string().end();

    match syncer_type {
        Some("localStorage") => {
            let path = syncer
                .chain(&mut err)
                .get_key("path")
                .parse_from_str("Expected checkpoint syncer file path")
                .end();
            cfg_unwrap_all!(&syncer.cwp, err: [path]);
            err.into_result(CheckpointSyncerConf::LocalStorage { path })
        }
        Some("s3") => {
            let bucket = syncer
                .chain(&mut err)
                .get_key("bucket")
                .parse_string()
                .end()
                .map(str::to_owned);
            let region: Option<String> = syncer
                .chain(&mut err)
                .get_key("region")
                .parse_string()
                .end()
                .map(str::to_owned);
            let folder = syncer
                .chain(&mut err)
                .get_opt_key("folder")
                .parse_string()
                .end()
                .map(str::to_owned);

            cfg_unwrap_all!(&syncer.cwp, err: [bucket, region]);
            err.into_result(CheckpointSyncerConf::S3 {
                bucket,
                region: Region::new(region),
                folder,
            })
        }
        Some("gcs") => {
            let bucket = syncer
                .chain(&mut err)
                .get_key("bucket")
                .parse_string()
                .end()
                .map(str::to_owned);
            let folder = syncer
                .chain(&mut err)
                .get_opt_key("folder")
                .parse_string()
                .end()
                .map(str::to_owned);
            let service_account_key = syncer
                .chain(&mut err)
                .get_opt_key("serviceAccountKey")
                .parse_string()
                .end()
                .map(str::to_owned);
            let user_secrets = syncer
                .chain(&mut err)
                .get_opt_key("userSecrets")
                .parse_string()
                .end()
                .map(str::to_owned);
            let use_application_default = syncer
                .chain(&mut err)
                .get_opt_key("useApplicationDefault")
                .parse_bool()
                .end()
                .unwrap_or(false);

            cfg_unwrap_all!(&syncer.cwp, err: [bucket]);
            err.into_result(CheckpointSyncerConf::Gcs {
                bucket,
                folder,
                service_account_key,
                user_secrets,
                use_application_default,
            })
        }
        Some(_) => Err(eyre!("Unknown checkpoint syncer type"))
            .into_config_result(|| (&syncer.cwp).add("type")),
        None => Err(err),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rpc_config_debug_never_contains_url_credentials_or_private_components() {
        let config = RpcConfig {
            url: Secret::new(
                "https://debug-user:debug-password@rpc.example/private-path?token=query-sentinel"
                    .to_owned(),
            ),
            public: false,
        };
        let rendered = format!("{config:?}");

        assert!(rendered.contains("[REDACTED"));
        for secret in [
            "debug-user",
            "debug-password",
            "private-path",
            "query-sentinel",
        ] {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn max_sign_concurrency_is_bounded() {
        let cwp = ConfigPath::default();
        let max_sign_concurrency =
            u64::try_from(MAX_SIGN_CONCURRENCY).expect("MAX_SIGN_CONCURRENCY must fit in u64");

        for (configured, expected) in [
            (None, DEFAULT_MAX_SIGN_CONCURRENCY),
            (Some(1), 1),
            (Some(max_sign_concurrency), MAX_SIGN_CONCURRENCY),
        ] {
            let mut err = ConfigParsingError::default();
            assert_eq!(
                parse_max_sign_concurrency(configured, &cwp, &mut err),
                expected
            );
            assert!(err.is_ok());
        }

        for configured in [0, max_sign_concurrency + 1, u64::MAX] {
            let mut err = ConfigParsingError::default();
            assert_eq!(
                parse_max_sign_concurrency(Some(configured), &cwp, &mut err),
                DEFAULT_MAX_SIGN_CONCURRENCY
            );
            assert!(!err.is_ok());
            assert!(err.to_string().contains("maxSignConcurrency"));
        }
    }

    #[test]
    fn test_get_rpc_urls_explicit() {
        let expected = [
            RpcConfig {
                url: "http://my-rpc-url.com".to_string().into(),
                public: true,
            },
            RpcConfig {
                url: "http://my-rpc-url-2.com".to_string().into(),
                public: false,
            },
        ];

        let rpcs = expected
            .iter()
            .map(|rpc| {
                serde_json::json!({
                    "http": rpc.url.expose_secret(),
                    "public": rpc.public
                })
            })
            .collect::<Vec<_>>();
        let rpcs = serde_json::json!({
            "rpcurls": rpcs
        });

        let mut err = ConfigParsingError::default();
        let value_parser = ValueParser::new(ConfigPath::default(), &rpcs);
        let parsed = get_rpc_urls(&value_parser, "rpcUrls", "customRpcUrls", &mut err); // why does it convert to lowercase?

        assert_eq!(parsed.len(), expected.len());
        for (i, rpc) in expected.iter().enumerate() {
            assert_eq!(parsed[i].url.expose_secret(), rpc.url.expose_secret());
            assert_eq!(parsed[i].public, rpc.public);
        }
    }

    #[test]
    fn test_get_rpc_urls_implicit_private() {
        let rpcs = r#"
            {
                "rpcurls": [
                    {
                        "http": "http://my-rpc-url.com"
                    },
                    {
                        "http": "http://my-rpc-url-2.com",
                        "public": false
                    }
                ]
            }
        "#;
        let rpcs = serde_json::from_str(rpcs).unwrap();
        let mut err = ConfigParsingError::default();
        let value_parser = ValueParser::new(ConfigPath::default(), &rpcs);
        let parsed = get_rpc_urls(&value_parser, "rpcUrls", "customRpcUrls", &mut err);

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].url.expose_secret(), "http://my-rpc-url.com");
        assert!(!parsed[0].public);
        assert_eq!(parsed[1].url.expose_secret(), "http://my-rpc-url-2.com");
        assert!(!parsed[1].public);
    }

    #[test]
    fn test_get_rpc_urls_overrides() {
        let rpcs = r#"
            {
                "rpcurls": [
                    {
                        "http": "http://my-rpc-url.com"
                    },
                    {
                        "http": "http://my-rpc-url-2.com",
                        "public": false
                    }
                ],
                "customrpcurls": "http://my-rpc-url-3.com,http://my-rpc-url-4.com"
            }
        "#;
        let rpcs = serde_json::from_str(rpcs).unwrap();
        let mut err = ConfigParsingError::default();
        let value_parser = ValueParser::new(ConfigPath::default(), &rpcs);
        let parsed = get_rpc_urls(&value_parser, "rpcUrls", "customRpcUrls", &mut err);

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].url.expose_secret(), "http://my-rpc-url-3.com");
        assert!(!parsed[0].public);
        assert_eq!(parsed[1].url.expose_secret(), "http://my-rpc-url-4.com");
        assert!(!parsed[1].public);
    }

    fn validator_settings_fixture() -> Value {
        serde_json::json!({
            "originchainname": "test",
            "websocketurl": "wss://scraper.example/validators",
            "validator": {"type": "hexKey", "key": format!("0x{}", "11".repeat(32))},
            "checkpointsyncer": {"type": "localStorage", "path": "/tmp/validator-checkpoints"},
            "chains": {"test": {
                "name": "test", "domainid": 1337, "chainid": 1337, "protocol": "ethereum",
                "rpcurls": [
                    {"http": "https://public.example", "public": true},
                    {"http": "https://private.example", "public": false}
                ],
                "customrpcurls": "https://private-override.example",
                "mailbox": "0x0000000000000000000000000000000000000001",
                "interchaingaspaymaster": "0x0000000000000000000000000000000000000002",
                "validatorannounce": "0x0000000000000000000000000000000000000003",
                "merkletreehook": "0x0000000000000000000000000000000000000004"
            }}
        })
    }

    #[test]
    fn rejects_invalid_validator_websocket_paths_at_startup() {
        for url in [
            "wss://scraper.example/agents",
            "wss://scraper.example/validators?domain=1",
        ] {
            let mut raw = validator_settings_fixture();
            raw["websocketurl"] = url.into();
            let result = ValidatorSettings::from_config_filtered(
                RawValidatorSettings(raw.into()),
                &ConfigPath::default(),
                (),
                "validator",
            );
            assert!(result.is_err());
        }
    }

    #[test]
    fn checkpoint_consensus_is_protocol_independent() {
        for protocol in [
            "ethereum",
            "sealevel",
            "cosmos",
            "cosmosnative",
            "starknet",
            "radix",
            "tron",
            #[cfg(feature = "aleo")]
            "aleo",
        ] {
            for (mode, expected) in [
                ("quorum", Some(CheckpointConsensus::Quorum)),
                ("majority", Some(CheckpointConsensus::Majority)),
                ("single", None),
                ("fallback", None),
            ] {
                let mut raw = validator_settings_fixture();

                let chain = &mut raw["chains"]["test"];
                chain["protocol"] = protocol.into();
                chain["rpcconsensustype"] = mode.into();
                chain["chainid"] = if protocol.starts_with("cosmos") {
                    "test-1"
                } else {
                    "1337"
                }
                .into();
                chain["grpcurls"] = serde_json::json!([{"http":"https://grpc-a.example"},{"http":"https://grpc-b.example"}]);
                chain["walleturls"] = serde_json::json!([{"http":"https://wallet.example"}]);
                chain["walletsolidityurls"] = serde_json::json!([{"http":"https://solid-a.example"},{"http":"https://solid-b.example"}]);
                chain["gatewayurls"] = serde_json::json!([{"http":"https://gateway.example"}]);
                chain["bech32prefix"] = "test".into();
                chain["gasprice"] = serde_json::json!({"denom":"utest","amount":"0.1"});
                chain["contractaddressbytes"] = 32.into();
                chain["networkname"] = "mainnet".into();
                chain["nativetoken"] = serde_json::json!({"denom":"0x0000000000000000000000000000000000000005","decimals":18,"symbol":"TEST"});
                chain["mailboxprogram"] = "mailbox.aleo".into();
                chain["hookmanagerprogram"] = "hooks.aleo".into();
                chain["ismmanagerprogram"] = "isms.aleo".into();
                chain["validatorannounceprogram"] = "announce.aleo".into();
                let settings = ValidatorSettings::from_config_filtered(
                    RawValidatorSettings(raw.into()),
                    &ConfigPath::default(),
                    (),
                    "validator",
                )
                .unwrap_or_else(|err| panic!("{protocol}/{mode}: {err}"));
                assert_eq!(settings.checkpoint_consensus, expected);
                if expected.is_some() {
                    if let hyperlane_base::settings::ChainConnectionConf::Ethereum(conn) =
                        &settings.chains[&settings.origin_chain].connection
                    {
                        assert!(matches!(
                            conn.rpc_connection,
                            hyperlane_ethereum::RpcConnectionConf::HttpFallback { .. }
                        ));
                    }
                }
            }
        }
        let raw = validator_settings_fixture();

        let settings = ValidatorSettings::from_config_filtered(
            RawValidatorSettings(raw.into()),
            &ConfigPath::default(),
            (),
            "validator",
        )
        .unwrap();
        assert_eq!(
            settings.checkpoint_consensus,
            Some(CheckpointConsensus::Majority)
        );
    }

    #[test]
    fn removed_quorum_settings_require_migration() {
        for (key, value) in [
            (
                "additionalquorumrpcurls",
                serde_json::json!([{"http": "https://quorum.example"}]),
            ),
            ("additionalquorumrpcurls", serde_json::json!([])),
            (
                "customadditionalquorumrpcurls",
                serde_json::json!("https://quorum.example"),
            ),
            ("customadditionalquorumrpcurls", serde_json::json!("")),
        ] {
            let mut raw = validator_settings_fixture();

            raw["chains"]["test"][key] = value;
            let error = ValidatorSettings::from_config_filtered(
                RawValidatorSettings(raw.into()),
                &ConfigPath::default(),
                (),
                "validator",
            )
            .expect_err("obsolete quorum configuration must not be silently ignored");
            assert!(error.to_string().contains("was removed"));
        }
    }

    #[test]
    fn accepts_non_evm_validator_configuration() {
        let mut raw = validator_settings_fixture();
        raw["chains"]["test"]["protocol"] = Value::String("sealevel".into());
        let settings = ValidatorSettings::from_config_filtered(
            RawValidatorSettings(raw.into()),
            &ConfigPath::default(),
            (),
            "validator",
        )
        .expect("non-EVM validator");
        assert_eq!(
            settings.origin_chain.domain_protocol(),
            HyperlaneDomainProtocol::Sealevel
        );
    }

    #[test]
    fn keeps_all_rpc_urls_even_with_single_consensus() {
        for consensus in ["single", "fallback", "quorum", "majority"] {
            let mut raw = validator_settings_fixture();
            raw["chains"]["test"]
                .as_object_mut()
                .expect("chain object")
                .remove("customrpcurls");
            raw["chains"]["test"]["rpcconsensustype"] = consensus.into();
            let settings = ValidatorSettings::from_config_filtered(
                RawValidatorSettings(raw.into()),
                &ConfigPath::default(),
                (),
                "validator",
            )
            .expect("validator configuration");
            assert_eq!(settings.rpcs.len(), 2);
            assert!(settings.rpcs[0].public);
            assert!(!settings.rpcs[1].public);
        }
    }

    #[test]
    fn metadata_includes_cosmos_and_tron_state_read_transports() {
        use crate::validator::ValidatorMetadata;
        use hyperlane_base::MetadataFromSettings;

        for protocol in ["cosmos", "cosmosnative", "tron"] {
            let mut raw = validator_settings_fixture();

            let chain = &mut raw["chains"]["test"];
            chain["protocol"] = protocol.into();
            chain["chainid"] = if protocol.starts_with("cosmos") {
                "test-1"
            } else {
                "1337"
            }
            .into();
            chain["bech32prefix"] = "test".into();
            chain["gasprice"] = serde_json::json!({"denom": "utest", "amount": "0.1"});
            chain["contractaddressbytes"] = 32.into();
            chain["grpcurls"] = serde_json::json!([{"http": "https://grpc-registry.example"}]);
            chain["customgrpcurls"] = "https://grpc-private.example".into();
            chain["walleturls"] = serde_json::json!([{"http": "https://wallet.example"}]);
            chain["walletsolidityurls"] =
                serde_json::json!([{"http": "https://solid-registry.example"}]);
            chain["customwalletsolidityurls"] = "https://solid-private.example".into();
            let settings = ValidatorSettings::from_config_filtered(
                RawValidatorSettings(raw.into()),
                &ConfigPath::default(),
                (),
                "validator",
            )
            .expect("valid transport configuration");
            let metadata = serde_json::to_value(ValidatorMetadata::build_metadata(&settings))
                .expect("serialized metadata");
            let hashes: Vec<_> = metadata["rpcs"]
                .as_array()
                .expect("RPC metadata")
                .iter()
                .map(|entry| entry["url_hash"].clone())
                .collect();
            for url in [
                "https://private-override.example",
                "https://grpc-private.example",
                "https://wallet.example",
                "https://solid-private.example",
            ] {
                let expected =
                    serde_json::to_value(hyperlane_core::H256::from(ethers::utils::keccak256(url)))
                        .expect("hash");
                assert!(hashes.contains(&expected), "{protocol}: missing transport");
            }
            assert_eq!(hashes.len(), 4);
        }
    }

    /// Parse real validator settings so interval cases exercise startup validation and precedence.
    fn parse_with_intervals(
        top_level: Option<Value>,
        chain_index: Option<Value>,
    ) -> ConfigResult<ValidatorSettings> {
        let mut raw = validator_settings_fixture();

        if let Some(interval) = top_level {
            raw["interval"] = interval;
        }
        if let Some(interval) = chain_index {
            raw["chains"]["test"]["index"] = serde_json::json!({ "interval": interval });
        }
        ValidatorSettings::from_config_filtered(
            RawValidatorSettings(raw.into()),
            &ConfigPath::default(),
            (),
            "validator",
        )
    }

    /// Top-level `interval` wins over the origin chain's `index.interval`, which wins over the 2s
    /// default; both are seconds and accept fractions as numbers or strings.
    #[test]
    fn interval_precedence_accepts_fractional_seconds() {
        use serde_json::json;
        let ms = Duration::from_millis;
        for (top_level, chain_index, expected) in [
            (None, None, Duration::from_secs(2)),
            (Some(json!(0.1)), None, ms(100)),
            (Some(json!("0.1")), None, ms(100)),
            (None, Some(json!(0.1)), ms(100)),
            (None, Some(json!("0.1")), ms(100)),
            (Some(json!(0.25)), Some(json!(3)), ms(250)),
            (Some(json!(3)), Some(json!(0.25)), Duration::from_secs(3)),
            (Some(json!("1.5")), Some(json!("0.1")), ms(1500)),
            (None, Some(json!(7)), Duration::from_secs(7)),
            (Some(json!(0.01)), None, ms(10)),
            (None, Some(json!("0.01")), ms(10)),
        ] {
            let case = format!("top={top_level:?} chain={chain_index:?}");
            let settings = parse_with_intervals(top_level, chain_index)
                .unwrap_or_else(|err| panic!("{case}: {err}"));
            assert_eq!(settings.interval, expected, "{case}");
        }
    }

    /// Invalid values are rejected wherever they appear, even when the other key is valid, so a
    /// typo never silently falls back to another interval or becomes a near-zero wait.
    #[test]
    fn interval_rejects_nonpositive_malformed_and_unrepresentable_values() {
        use serde_json::json;
        for invalid in [
            json!(0),
            json!("0"),
            json!(0.0),
            json!(-1),
            json!(-0.1),
            json!("-0.1"),
            json!(1e-10),
            json!("1e-10"),
            json!(0.009),
            json!("0.009"),
            json!(1e20),
            json!("1e400"),
            json!("NaN"),
            json!("inf"),
            json!(""),
            json!("abc"),
            json!(true),
        ] {
            let top = "config_path: `interval`";
            let chain = "config_path: `chains.test.index.interval`";
            for (top_level, chain_index, path) in [
                (Some(invalid.clone()), None, top),
                (Some(invalid.clone()), Some(json!(0.1)), top),
                (None, Some(invalid.clone()), chain),
                (Some(json!(0.1)), Some(invalid.clone()), chain),
            ] {
                let case = format!("top={top_level:?} chain={chain_index:?}");
                let error = parse_with_intervals(top_level, chain_index)
                    .expect_err(&format!("{case} must be rejected"))
                    .to_string();
                assert!(error.contains(path), "{case}: {error}");
            }
        }
    }

    #[test]
    fn removed_lightweight_settings_require_rpc_indexing() {
        for flag in ["lightweight", "leightweigt"] {
            for value in [
                Value::Bool(true),
                Value::Bool(false),
                Value::String(String::new()),
            ] {
                let mut raw = validator_settings_fixture();
                raw[flag] = value;
                let error = ValidatorSettings::from_config_filtered(
                    RawValidatorSettings(raw.into()),
                    &ConfigPath::default(),
                    (),
                    "validator",
                )
                .expect_err("removed setting");
                assert!(error.to_string().contains("require RPC indexing"));
            }
        }
    }
}

#[cfg(test)]
mod dusk_rpc_tests {
    //! Dusk checkpoint endpoints retain their voting slots while offline and must
    //! authenticate chain and contract identities before producing state reads.
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc,
    };
    use std::thread::{self, JoinHandle};
    use std::time::Duration;

    use hyperlane_base::{settings::ChainConf, CoreMetrics};
    use hyperlane_core::{
        config::{ConfigPath, FromRawConf},
        CheckpointAtBlock, MerkleTreeHook, ReorgPeriod, H256,
    };
    use hyperlane_metric::prometheus_metric::RpcRole;
    use prometheus::Registry;
    use serde_json::Value;
    use tempfile::TempDir;
    use url::Url;

    use crate::checkpoint_consensus::{CheckpointConsensus, CheckpointReader};
    use crate::reorg_reporter::{LatestCheckpointReorgReporter, ReorgReporter};
    use crate::rpc::build_validator_per_url_hooks;
    use crate::settings::{RawValidatorSettings, ValidatorSettings};

    const DOMAIN: u32 = 1337;
    const CHAIN_ID: u8 = 7;
    /// The one message ID the fake MerkleTreeHook stores.
    const LEAF: [u8; 32] = [9; 32];
    const READ_DEADLINE: Duration = Duration::from_secs(5);

    #[derive(Clone, Copy)]
    #[repr(u8)]
    enum Fault {
        None,
        Unavailable,
        WrongChain,
        WrongMailboxDomain,
        WrongAnnounceDomain,
        WrongAnnounceMailbox,
        WrongMerkleMailbox,
        WrongRequiredHook,
        AnnounceUnavailable,
        /// A Mailbox that still stores the processed-message log (state
        /// version 4).
        OldMailbox,
    }

    struct Node {
        url: Url,
        fault: Arc<AtomicU8>,
        identity_reads: Arc<AtomicUsize>,
        state_reads: Arc<AtomicUsize>,
        unavailable_responses: Arc<AtomicUsize>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl Node {
        fn start(initial: Fault) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture listener");
            listener
                .set_nonblocking(true)
                .expect("make fixture listener nonblocking");
            let url = Url::parse(&format!(
                "http://{}",
                listener.local_addr().expect("get fixture address")
            ))
            .expect("parse fixture URL");
            let fault = Arc::new(AtomicU8::new(initial as u8));
            let identity_reads = Arc::new(AtomicUsize::new(0));
            let state_reads = Arc::new(AtomicUsize::new(0));
            let unavailable_responses = Arc::new(AtomicUsize::new(0));
            let observed_unavailable = unavailable_responses.clone();
            let stop = Arc::new(AtomicBool::new(false));
            let observed_fault = fault.clone();
            let observed_identity = identity_reads.clone();
            let observed_state = state_reads.clone();
            let stopped = stop.clone();
            let worker = thread::spawn(move || {
                while !stopped.load(Ordering::SeqCst) {
                    let (mut stream, _) = match listener.accept() {
                        Ok(connection) => connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    };
                    stream
                        .set_nonblocking(false)
                        .expect("make request socket blocking");
                    stream
                        .set_read_timeout(Some(READ_DEADLINE))
                        .expect("bound fixture request reads");
                    let mut reader = BufReader::new(&mut stream);
                    let mut request = String::new();
                    if reader.read_line(&mut request).expect("read HTTP request") == 0 {
                        continue;
                    }
                    let path = request
                        .split_whitespace()
                        .nth(1)
                        .expect("HTTP request has a target")
                        .to_owned();
                    let mut length = 0;
                    loop {
                        let mut header = String::new();
                        assert_ne!(
                            reader.read_line(&mut header).expect("read HTTP header"),
                            0,
                            "request ended before its headers"
                        );
                        if header == "\r\n" {
                            break;
                        }
                        if let Some(value) =
                            header.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            length = value.trim().parse().expect("parse Content-Length");
                        }
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).expect("read HTTP body");
                    drop(reader);

                    let chain_query = path.ends_with("/chain_id");
                    let mailbox_query = path.ends_with(&format!("{:064x}/local_domain", 1));
                    let announce_query = path.ends_with(&format!("{:064x}/local_domain", 3));
                    let topology_query = path.ends_with("/mailbox")
                        || path.ends_with("/required_hook")
                        || path.ends_with("/hook_type")
                        || path.ends_with("/state_version");
                    if chain_query || mailbox_query || announce_query || topology_query {
                        observed_identity.fetch_add(1, Ordering::SeqCst);
                    } else {
                        observed_state.fetch_add(1, Ordering::SeqCst);
                    }
                    let current = observed_fault.load(Ordering::SeqCst);
                    let (status, reply) = if current == Fault::Unavailable as u8 {
                        (503, b"temporarily unavailable".to_vec())
                    } else if chain_query {
                        (
                            200,
                            vec![if current == Fault::WrongChain as u8 {
                                CHAIN_ID + 1
                            } else {
                                CHAIN_ID
                            }],
                        )
                    } else if mailbox_query || announce_query {
                        let mismatched = (mailbox_query
                            && current == Fault::WrongMailboxDomain as u8)
                            || (announce_query && current == Fault::WrongAnnounceDomain as u8);
                        (
                            200,
                            (if mismatched { 1338u32 } else { DOMAIN })
                                .to_le_bytes()
                                .to_vec(),
                        )
                    } else if path.ends_with("/required_hook") {
                        (
                            200,
                            H256::from_low_u64_be(if current == Fault::WrongRequiredHook as u8 {
                                9
                            } else {
                                4
                            })
                            .as_bytes()
                            .to_vec(),
                        )
                    } else if path.ends_with("/hook_type") {
                        (200, vec![3])
                    } else if path.ends_with(&format!("{:064x}/state_version", 1)) {
                        let version: u32 = if current == Fault::OldMailbox as u8 {
                            5
                        } else {
                            6
                        };
                        (200, version.to_le_bytes().to_vec())
                    } else if path.ends_with(&format!("{:064x}/state_version", 3)) {
                        // ValidatorAnnounce.
                        (200, 3u32.to_le_bytes().to_vec())
                    } else if path.ends_with("/state_version") {
                        // MerkleTreeHook.
                        (200, 2u32.to_le_bytes().to_vec())
                    } else if path.ends_with("/mailbox") {
                        let wrong = (path.ends_with(&format!("{:064x}/mailbox", 3))
                            && current == Fault::WrongAnnounceMailbox as u8)
                            || (path.ends_with(&format!("{:064x}/mailbox", 4))
                                && current == Fault::WrongMerkleMailbox as u8);
                        (
                            200,
                            H256::from_low_u64_be(if wrong { 9 } else { 1 })
                                .as_bytes()
                                .to_vec(),
                        )
                    } else if path == "/on/graphql/query"
                        && String::from_utf8_lossy(&body).contains("finalizedEvents(")
                    {
                        // An archive with no announcement yet.
                        (200, br#"{"finalizedEvents":{"json":{"events":[],"startCursor":null,"endCursor":null,"hasNextPage":false}}}"#.to_vec())
                    } else if path == "/on/graphql/query"
                        && String::from_utf8_lossy(&body).contains("checkBlock(")
                    {
                        // The archive has finalized the node's last final block.
                        (200, br#"{"checkBlock":true}"#.to_vec())
                    } else if path == "/on/graphql/query" {
                        assert_eq!(body, b"query { lastBlockPair { json } }");
                        (200, format!(r#"{{"lastBlockPair":{{"json":{{"last_block":[100,"{}"],"last_finalized_block":[90,"{}"]}}}}}}"#, "77".repeat(32), "ff".repeat(32)).into_bytes())
                    } else if path.ends_with("/get_announced_storage_location_hashes") {
                        if current == Fault::AnnounceUnavailable as u8 {
                            (503, b"temporarily unavailable".to_vec())
                        } else {
                            // rkyv archived empty Vec<[u8; 32]>: zero offset and length.
                            (200, vec![0; 8])
                        }
                    } else if path.ends_with("/count") {
                        (200, 1u32.to_le_bytes().to_vec())
                    } else if path.ends_with("/inserted_block_height") {
                        (200, 85u64.to_le_bytes().to_vec())
                    } else if path.ends_with("/message_ids") {
                        // rkyv Vec<[u8; 32]> of one ID: the ID, then a
                        // relative pointer back to it and the length.
                        let mut reply = LEAF.to_vec();
                        reply.extend_from_slice(&(-32i32).to_le_bytes());
                        reply.extend_from_slice(&1u32.to_le_bytes());
                        (200, reply)
                    } else if path.ends_with("/message_id_at") {
                        (200, LEAF.to_vec())
                    } else if path.ends_with("/tree") {
                        // The hook's tree of one leaf: the leaf at level 0,
                        // zero above it, and the count.
                        let mut reply = LEAF.to_vec();
                        reply.resize(32 * 32, 0);
                        reply.extend_from_slice(&1u32.to_le_bytes());
                        (200, reply)
                    } else {
                        panic!("unexpected fixture path {path}");
                    };
                    if status == 503 {
                        observed_unavailable.fetch_add(1, Ordering::SeqCst);
                    }
                    write!(
                        stream,
                        "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        reply.len()
                    )
                    .expect("write HTTP response headers");
                    stream.write_all(&reply).expect("write HTTP response body");
                }
            });
            Self {
                url,
                fault,
                identity_reads,
                state_reads,
                unavailable_responses,
                stop,
                worker: Some(worker),
            }
        }

        fn set_fault(&self, fault: Fault) {
            self.fault.store(fault as u8, Ordering::SeqCst);
        }
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Err(panic) = self.worker.take().expect("fixture owns its worker").join() {
                if !thread::panicking() {
                    std::panic::resume_unwind(panic);
                }
            }
        }
    }

    struct Configuration {
        chain: ChainConf,
        validator: ValidatorSettings,
        _directory: TempDir,
    }

    impl Configuration {
        fn new(urls: &[Url]) -> Self {
            let directory = tempfile::tempdir().expect("create fixture directory");
            let raw: Value = serde_json::json!({
                "originchainname": "test",
                "validator": {"type": "hexKey", "key": format!("0x{}", "11".repeat(32))},
                "checkpointsyncer": {"type": "localStorage", "path": directory.path().join("checkpoints")},
                "db": directory.path().join("validator-db"),
                "chains": {"test": {
                    "name": "test", "domainid": DOMAIN, "chainid": CHAIN_ID, "protocol": "dusk",
                    "rpcconsensustype": "majority",
                    "rpcurls": urls.iter().map(|url| serde_json::json!({"http": url.as_str()})).collect::<Vec<_>>(),
                    "eventcursordir": directory.path().join("events"),
                    "nativetoken": {"decimals": 9, "symbol": "DUSK", "denom": "LUX"},
                    "mailbox": "0x0000000000000000000000000000000000000001",
                    "interchaingaspaymaster": "0x0000000000000000000000000000000000000002",
                    "validatorannounce": "0x0000000000000000000000000000000000000003",
                    "merkletreehook": "0x0000000000000000000000000000000000000004"
                }}
            });
            let validator = ValidatorSettings::from_config_filtered(
                RawValidatorSettings(redact::Secret::new(raw)),
                &ConfigPath::default(),
                (),
                "validator",
            )
            .expect("parse validator settings");
            let chain = validator.base.chains[&validator.origin_chain].clone();
            assert_eq!(validator.rpcs.len(), urls.len());
            Self {
                chain,
                validator,
                _directory: directory,
            }
        }
    }

    impl Configuration {
        async fn build_validator(&self) -> eyre::Result<crate::validator::Validator> {
            use crate::validator::{Validator, ValidatorMetadata};
            use hyperlane_base::{
                metrics::AgentMetrics, BaseAgent, ChainMetrics, MetadataFromSettings,
                RuntimeMetrics,
            };
            let metrics = metrics();
            let agent_metrics = AgentMetrics::new(&metrics).expect("agent metrics");
            let chain_metrics = ChainMetrics::new(&metrics).expect("chain metrics");
            let runtime_metrics =
                RuntimeMetrics::new(&metrics, Default::default()).expect("runtime metrics");
            let (_, console_server) = console_subscriber::ConsoleLayer::builder().build();
            Validator::from_settings(
                ValidatorMetadata::build_metadata(&self.validator),
                self.validator.clone(),
                metrics,
                agent_metrics,
                chain_metrics,
                runtime_metrics,
                console_server,
            )
            .await
        }
    }

    fn metrics() -> Arc<CoreMetrics> {
        Arc::new(
            CoreMetrics::new("dusk-rpc-fixture", 0, Registry::new())
                .expect("create fixture metrics"),
        )
    }

    fn assert_checkpoint(checkpoint: &CheckpointAtBlock) {
        assert_eq!(checkpoint.block_height, Some(90));
        assert_eq!(checkpoint.checkpoint.mailbox_domain, DOMAIN);
        assert_eq!(
            checkpoint.checkpoint.merkle_tree_hook_address,
            H256::from_low_u64_be(4)
        );
        assert_eq!(checkpoint.checkpoint.index, 0);
        let mut tree = hyperlane_core::accumulator::incremental::IncrementalMerkle::default();
        tree.ingest(H256::from(LEAF));
        assert_eq!(checkpoint.checkpoint.root, tree.root());
    }

    async fn build_hooks(
        configuration: &Configuration,
        urls: &[Url],
    ) -> Vec<Arc<dyn MerkleTreeHook>> {
        tokio::time::timeout(
            READ_DEADLINE,
            build_validator_per_url_hooks(
                &configuration.chain,
                "rpcUrls",
                RpcRole::Primary,
                urls,
                &metrics(),
            ),
        )
        .await
        .expect("endpoint pool construction finishes")
        .expect("offline secondary does not veto endpoint pool")
        .into_iter()
        .map(|(_, hook)| hook)
        .collect()
    }

    #[test]
    fn dusk_endpoint_deduplication_uses_request_url_semantics() {
        use crate::rpc::{dedupe_rpc_urls, state_read_urls};
        let urls: Vec<Url> = [
            "https://rpc.example/rpc",
            "https://rpc.example/rpc/",
            "https://rpc.example/rpc/#display",
            "https://other.example/rpc",
            "https://rpc.example/rpc?token=one/",
            "https://rpc.example/rpc?token=one",
            "https://reader:fixture@rpc.example/rpc",
            "https://rpc.example/encoded%2Fpath",
        ]
        .iter()
        .map(|url| Url::parse(url).expect("parse fixture URL"))
        .collect();
        let configuration = Configuration::new(&urls);
        let (source, selected) = state_read_urls(&configuration.chain, urls.clone());
        assert_eq!(
            dedupe_rpc_urls(selected, source),
            [
                urls[0].clone(),
                urls[3].clone(),
                urls[4].clone(),
                urls[5].clone(),
                urls[6].clone(),
                urls[7].clone()
            ]
        );
    }

    #[tokio::test]
    async fn dusk_validator_builds_without_reaching_the_node_and_waits_on_its_first_reads() {
        // Classic RPC indexing and WebSocket with RPC recovery both use the
        // real validator construction sequence.
        for mode in 0..2 {
            let node = Node::start(Fault::Unavailable);
            let mut configuration = Configuration::new(&[node.url.clone()]);
            configuration.validator.interval = Duration::from_millis(10);
            if mode > 0 {
                configuration.validator.websocket_url = Some(
                    "ws://127.0.0.1:1/fixture"
                        .parse()
                        .expect("fixture websocket URL"),
                );
            }
            let _validator = tokio::time::timeout(READ_DEADLINE, configuration.build_validator())
                .await
                .expect("construction makes no request")
                .expect("validator builds while the node is unavailable");
            assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
            assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);

            // The next step of Validator::run reads announcement state: it
            // fails, without reaching that state, until the node is back and
            // serves the configured deployment.
            let reader = configuration
                .chain
                .build_validator_self_announce_reader(&metrics(), "file:///fixture/checkpoints")
                .await
                .expect("reader builds while the node is unavailable");
            let validators = [H256::repeat_byte(7)];
            assert!(reader
                .get_announced_storage_locations(&validators)
                .await
                .is_err());
            assert!(node.unavailable_responses.load(Ordering::SeqCst) > 0);
            assert_eq!(
                node.state_reads.load(Ordering::SeqCst),
                0,
                "unverified startup cannot reach announcement or checkpoint state"
            );
            node.set_fault(Fault::None);
            assert_eq!(
                reader
                    .get_announced_storage_locations(&validators)
                    .await
                    .expect("verified reader observes announcement state"),
                vec![Vec::<String>::new()]
            );
            assert_eq!(node.identity_reads.load(Ordering::SeqCst), 11);
        }
    }

    #[tokio::test]
    async fn dusk_local_initialization_errors_are_reported_at_construction() {
        use hyperlane_base::settings::{ChainConnectionConf, DuskSignerKeyConf, SignerConf};

        let node = Node::start(Fault::None);
        let mut configuration = Configuration::new(&[node.url.clone()]);
        configuration.validator.interval = Duration::from_millis(10);
        let invalid_directory = configuration._directory.path().join("events-is-a-file");
        std::fs::write(&invalid_directory, b"fixture").expect("create ordinary path conflict");
        if let ChainConnectionConf::Dusk(connection) = &mut configuration.chain.connection {
            connection.event_cursor_dir = invalid_directory;
        } else {
            panic!("fixture must select Dusk");
        }
        configuration.validator.base.chains.insert(
            configuration.validator.origin_chain.clone(),
            configuration.chain.clone(),
        );
        tokio::time::timeout(READ_DEADLINE, configuration.build_validator())
            .await
            .expect("local indexer error returns promptly")
            .expect_err("an event-store path must be usable");
        let metrics = metrics();
        tokio::time::timeout(
            READ_DEADLINE,
            configuration
                .chain
                .build_validator_self_announce_reader(&metrics, "file:///fixture/checkpoints"),
        )
        .await
        .expect("local announcement-reader error returns promptly")
        .expect_err("an event-store path must be usable");
        tokio::time::timeout(
            READ_DEADLINE,
            configuration.chain.build_validator_announce(&metrics),
        )
        .await
        .expect("local announcement-client error returns promptly")
        .expect_err("an event-store path must be usable");

        // An invalid chain key fails the signer-backed factories promptly.
        let mut configuration = Configuration::new(&[node.url.clone()]);
        configuration.chain.signer = Some(SignerConf::DuskKey {
            key: DuskSignerKeyConf::Inline {
                key: "0x00".to_owned(),
            },
        });
        tokio::time::timeout(
            READ_DEADLINE,
            configuration.chain.build_validator_announce(&metrics),
        )
        .await
        .expect("submission signer error returns promptly")
        .expect_err("invalid local signer must fail");
        assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn dusk_identity_failures_block_every_provider_read_and_can_recover() {
        use hyperlane_core::HyperlaneProvider;

        for fault in [
            Fault::Unavailable,
            Fault::WrongChain,
            Fault::WrongAnnounceMailbox,
        ] {
            let node = Node::start(fault);
            let configuration = Configuration::new(&[node.url.clone()]);
            let provider: Box<dyn HyperlaneProvider> = configuration
                .chain
                .build_provider(&metrics())
                .await
                .expect("provider construction makes no request");
            assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
            let error = provider
                .get_block_by_height(1)
                .await
                .expect_err("unverified identity cannot serve a read")
                .to_string();
            assert!(error.contains("identity is not verified yet"), "{error}");
            assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn dusk_announcement_clients_wait_for_identity_on_their_first_read() {
        let metrics = metrics();
        for submission in [false, true] {
            let node = Node::start(Fault::Unavailable);
            let configuration = Configuration::new(&[node.url.clone()]);
            let client = if submission {
                configuration.chain.build_validator_announce(&metrics).await
            } else {
                configuration
                    .chain
                    .build_validator_self_announce_reader(&metrics, "file:///fixture/checkpoints")
                    .await
            }
            .expect("announcement clients build without a request");
            assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
            let validators = [H256::repeat_byte(7)];
            let result = client.get_announced_storage_locations(&validators).await;
            // A relayer-style reader reports a validator it cannot read as
            // having no locations; the validator's own check fails.
            if submission {
                assert_eq!(result.expect("partial results"), vec![Vec::<String>::new()]);
            } else {
                assert!(result.is_err());
            }
            assert_eq!(
                node.state_reads.load(Ordering::SeqCst),
                0,
                "unverified clients cannot reach announcement reads or writes"
            );
            node.set_fault(Fault::None);
            assert_eq!(
                client
                    .get_announced_storage_locations(&validators)
                    .await
                    .expect("valid Dusk announcement fixture result"),
                vec![Vec::<String>::new()]
            );
            assert!(node.state_reads.load(Ordering::SeqCst) > 0);
        }
    }

    #[tokio::test]
    async fn dusk_self_announce_reader_preserves_observation_errors_and_recovers() {
        let node = Node::start(Fault::None);
        let configuration = Configuration::new(&[node.url.clone()]);
        let metrics = metrics();
        let strict = configuration
            .chain
            .build_validator_self_announce_reader(&metrics, "file:///fixture/checkpoints")
            .await
            .expect("valid Dusk announcement fixture result");
        let partial = configuration
            .chain
            .build_validator_announce_reader(&metrics)
            .await
            .expect("valid Dusk announcement fixture result");
        node.set_fault(Fault::AnnounceUnavailable);
        let validators = [H256::repeat_byte(7)];
        assert!(strict
            .get_announced_storage_locations(&validators)
            .await
            .is_err());
        assert_eq!(
            partial
                .get_announced_storage_locations(&validators)
                .await
                .expect("valid Dusk announcement fixture result"),
            vec![Vec::<String>::new()]
        );
        node.set_fault(Fault::None);
        assert_eq!(
            strict
                .get_announced_storage_locations(&validators)
                .await
                .expect("valid Dusk announcement fixture result"),
            vec![Vec::<String>::new()]
        );
    }

    #[tokio::test]
    async fn dusk_unavailable_secondary_recovers_without_changing_quorum_denominator() {
        let nodes = [
            Node::start(Fault::None),
            Node::start(Fault::Unavailable),
            Node::start(Fault::None),
        ];
        let urls: Vec<_> = nodes.iter().map(|node| node.url.clone()).collect();
        let configuration = Configuration::new(&urls);
        let hooks = build_hooks(&configuration, &urls).await;
        assert_eq!(hooks.len(), 3);
        for node in &nodes {
            assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
            assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);
        }
        for consensus in [CheckpointConsensus::Majority, CheckpointConsensus::Quorum] {
            let reader = CheckpointReader::new(consensus, hooks.clone())
                .expect("configured pool is nonempty");
            assert_eq!(reader.endpoint_count(), 3);
            assert_eq!(reader.consensus.required(reader.endpoint_count()), 2);
            let votes = tokio::time::timeout(READ_DEADLINE, reader.checkpoints(&ReorgPeriod::None))
                .await
                .expect("checkpoint reads finish")
                .expect("two endpoints form a quorum");
            assert!(votes[1].is_none());
            assert_eq!(votes.iter().flatten().count(), 2);
            for checkpoint in votes.iter().flatten() {
                assert_checkpoint(checkpoint);
            }
            nodes[2].set_fault(Fault::Unavailable);
            // An endpoint's last answer stands until its clients read the
            // node's last final block again, 5 s later; then the unavailable
            // endpoint has no vote.
            tokio::time::sleep(Duration::from_millis(5_100)).await;
            assert!(
                tokio::time::timeout(READ_DEADLINE, reader.checkpoints(&ReorgPeriod::None))
                    .await
                    .expect("failed reads finish")
                    .is_err()
            );
            assert_eq!(reader.endpoint_count(), 3);
            nodes[2].set_fault(Fault::None);
        }
        let reader = CheckpointReader::new(CheckpointConsensus::Majority, hooks)
            .expect("configured pool is nonempty");
        nodes[1].set_fault(Fault::None);
        let votes = tokio::time::timeout(READ_DEADLINE, reader.checkpoints(&ReorgPeriod::None))
            .await
            .expect("recovered reads finish")
            .expect("recovered endpoint rejoins quorum");
        assert_eq!(votes.iter().flatten().count(), 3);
        for checkpoint in votes.iter().flatten() {
            assert_checkpoint(checkpoint);
        }
        assert_eq!(nodes[0].identity_reads.load(Ordering::SeqCst), 10);
        assert_eq!(nodes[2].identity_reads.load(Ordering::SeqCst), 10);
    }

    async fn identity_failure_blocks_every_read_and_can_recover(fault: Fault) {
        let node = Node::start(fault);
        let configuration = Configuration::new(std::slice::from_ref(&node.url));
        let hook = configuration
            .chain
            .build_merkle_tree_hook(&metrics())
            .await
            .expect("state hook construction is lazy");
        assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
        assert!(hook.tree(&ReorgPeriod::None).await.is_err());
        assert!(hook.count(&ReorgPeriod::None).await.is_err());
        assert!(hook.latest_checkpoint(&ReorgPeriod::None).await.is_err());
        assert!(hook.latest_checkpoint_at_block(90).await.is_err());
        assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);
        node.set_fault(Fault::None);
        let recovered =
            tokio::time::timeout(READ_DEADLINE, hook.latest_checkpoint(&ReorgPeriod::None))
                .await
                .expect("identity recovery read finishes")
                .expect("corrected endpoint returns a checkpoint");
        assert_checkpoint(&recovered);
        let successful_validation_count = node.identity_reads.load(Ordering::SeqCst);
        assert_checkpoint(
            &hook
                .latest_checkpoint_at_block(90)
                .await
                .expect("validated hook returns a checkpoint at height"),
        );
        assert_eq!(
            node.identity_reads.load(Ordering::SeqCst),
            successful_validation_count
        );
    }

    #[tokio::test]
    async fn dusk_wrong_chain_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::WrongChain).await;
    }

    #[tokio::test]
    async fn dusk_wrong_mailbox_domain_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::WrongMailboxDomain).await;
    }

    #[tokio::test]
    async fn dusk_wrong_announce_domain_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::WrongAnnounceDomain).await;
    }

    #[tokio::test]
    async fn dusk_wrong_announce_mailbox_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::WrongAnnounceMailbox).await;
    }

    #[tokio::test]
    async fn dusk_wrong_merkle_mailbox_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::WrongMerkleMailbox).await;
    }

    #[tokio::test]
    async fn dusk_old_mailbox_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::OldMailbox).await;
    }

    #[tokio::test]
    async fn dusk_wrong_required_hook_cannot_return_any_state_read() {
        identity_failure_blocks_every_read_and_can_recover(Fault::WrongRequiredHook).await;
    }

    #[tokio::test]
    async fn dusk_concurrent_first_reads_share_identity_validation() {
        let node = Node::start(Fault::None);
        let configuration = Configuration::new(std::slice::from_ref(&node.url));
        let hook = configuration
            .chain
            .build_merkle_tree_hook(&metrics())
            .await
            .expect("state hook construction is lazy");
        let (count, checkpoint) = tokio::join!(
            hook.count(&ReorgPeriod::None),
            hook.latest_checkpoint(&ReorgPeriod::None)
        );
        assert_eq!(count.expect("healthy hook returns count"), 1);
        assert_checkpoint(&checkpoint.expect("healthy hook returns checkpoint"));
        assert_eq!(node.identity_reads.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn dusk_reorg_reporter_construction_tolerates_unavailable_secondary() {
        let nodes = [
            Node::start(Fault::None),
            Node::start(Fault::Unavailable),
            Node::start(Fault::None),
        ];
        let urls: Vec<_> = nodes.iter().map(|node| node.url.clone()).collect();
        let configuration = Configuration::new(&urls);
        let hooks = build_hooks(&configuration, &urls).await;
        let storage = hyperlane_base::LocalStorage::new(
            configuration._directory.path().join("reorg-reports"),
            None,
        )
        .expect("report storage");
        let reporter =
            LatestCheckpointReorgReporter::new(urls.iter().cloned().zip(hooks), Arc::new(storage));
        for node in &nodes {
            assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
        }
        tokio::time::timeout(
            READ_DEADLINE,
            ReorgReporter::report_with_reorg_period(&reporter, &ReorgPeriod::None),
        )
        .await
        .expect("diagnostic reads finish");
        assert!(nodes[0].state_reads.load(Ordering::SeqCst) > 0);
        assert_eq!(nodes[1].state_reads.load(Ordering::SeqCst), 0);
        assert!(nodes[2].state_reads.load(Ordering::SeqCst) > 0);
    }

    #[tokio::test]
    async fn dusk_invalid_url_is_still_rejected_during_pool_construction() {
        let node = Node::start(Fault::None);
        let configuration = Configuration::new(std::slice::from_ref(&node.url));
        let urls = [
            node.url.clone(),
            Url::parse("file:///tmp/dusk-rpc-invalid").expect("parse unsupported URL fixture"),
        ];
        assert!(build_validator_per_url_hooks(
            &configuration.chain,
            "rpcUrls",
            RpcRole::Primary,
            &urls,
            &metrics()
        )
        .await
        .is_err());
        assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
    }
}
