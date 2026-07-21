use std::fmt;
use std::fs;
use std::io::Read;
use std::str::FromStr;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use ethers::core::k256::sha2::{Digest, Sha256};
use ethers::prelude::{AwsSigner, LocalWallet};
use ethers::utils::hex::ToHex;
use eyre::{bail, Context, Report};
use moka::future::Cache;
use rusoto_core::Region;
use rusoto_kms::KmsClient;
use tracing::instrument;

use hyperlane_core::{AccountAddressType, H256};

use super::aws_credentials::AwsChainCredentialsProvider;
use crate::types::utils;

const AWS_SIGNER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DUSK_SIGNER_KEY_FILE_BYTES: u64 = 16 * 1024;

/// Resolve an AWS region string into a `rusoto_core::Region` without relying on
/// rusoto's `FromStr` allowlist, which rejects regions added after rusoto was
/// last updated (e.g. `eu-central-2`).
fn resolve_kms_region(region: &str) -> Region {
    Region::from_str(region).unwrap_or_else(|_| Region::Custom {
        name: region.to_owned(),
        endpoint: format!("https://kms.{region}.amazonaws.com"),
    })
}

/// Cache of constructed AWS signers, keyed by (KMS key id, region), so independent call
/// sites needing the same signer share one `GetPublicKey` call instead of repeating it.
///
/// Uses moka's `try_get_with`, not a `tokio::sync::OnceCell`: moka coalesces concurrent
/// callers into one `init` attempt on both success and failure, so a KMS outage fails once
/// for everyone waiting instead of serializing a fresh `AWS_SIGNER_TIMEOUT`-long attempt
/// per waiter.
type AwsSignerCache = Cache<(String, String), AwsSigner>;

static AWS_SIGNER_CACHE: OnceLock<AwsSignerCache> = OnceLock::new();

fn get_aws_signer_cache() -> &'static AwsSignerCache {
    AWS_SIGNER_CACHE.get_or_init(|| Cache::builder().max_capacity(100).build())
}

/// Builds an `AwsSigner` for the given KMS key id and region, reusing an already-constructed
/// signer for the same (id, region) pair if one exists rather than making a fresh KMS call.
async fn build_aws_signer(id: &str, region: &str) -> Result<AwsSigner, Report> {
    get_aws_signer_cache()
        .try_get_with((id.to_owned(), region.to_owned()), async {
            let http_client =
                utils::http_client_with_timeout().map_err(|err| eyre::eyre!(err.to_string()))?;
            let client = KmsClient::new_with_client(
                rusoto_core::Client::new_with(AwsChainCredentialsProvider::new(), http_client),
                resolve_kms_region(region),
            );
            AwsSigner::new(client, id, 0, Some(AWS_SIGNER_TIMEOUT))
                .await
                .map_err(Report::from)
        })
        .await
        .map_err(|arc_err| eyre::eyre!("{arc_err}"))
}

/// Dusk signer key material source.
#[derive(Clone)]
pub enum DuskSignerKeyConf {
    /// Inline raw key material. Supported for backwards compatibility and local
    /// dev only; prefer File or Env for review/CI configs.
    Inline {
        /// Private key value
        key: String,
    },
    /// Path to a file containing hex/base58/bech32 raw key material.
    File {
        /// File path
        path: String,
    },
    /// Environment variable containing hex/base58/bech32 raw key material.
    Env {
        /// Environment variable name
        var: String,
    },
}

impl fmt::Debug for DuskSignerKeyConf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline { .. } => f
                .debug_struct("Inline")
                .field("key", &"<redacted>")
                .finish(),
            Self::File { path } => f.debug_struct("File").field("path", path).finish(),
            Self::Env { var } => f.debug_struct("Env").field("var", var).finish(),
        }
    }
}

impl DuskSignerKeyConf {
    fn resolve(&self) -> Result<H256, Report> {
        match self {
            Self::Inline { key } => {
                Self::parse_key_material(key).context("Invalid inline Dusk signer key")
            }
            Self::File { path } => {
                let key = Self::read_key_file(path)?;
                Self::parse_key_material(&key)
                    .with_context(|| format!("Invalid Dusk signer key file `{path}`"))
            }
            Self::Env { var } => {
                let key = std::env::var(var)
                    .with_context(|| format!("Dusk signer key env var `{var}` is not set"))?;
                Self::parse_key_material(&key)
                    .with_context(|| format!("Invalid Dusk signer key env var `{var}`"))
            }
        }
    }

    fn read_key_file(path: &str) -> Result<String, Report> {
        // Inspect and read through the same open handle. This avoids a path
        // replacement race between permission validation and key loading.
        let file = fs::File::open(path)
            .with_context(|| format!("Failed to open Dusk signer key file `{path}`"))?;
        let metadata = file
            .metadata()
            .with_context(|| format!("Failed to inspect Dusk signer key file `{path}`"))?;

        if !metadata.is_file() {
            bail!("Dusk signer key file `{path}` must be a regular file");
        }

        if metadata.len() > MAX_DUSK_SIGNER_KEY_FILE_BYTES {
            bail!("Dusk signer key file `{path}` exceeds {MAX_DUSK_SIGNER_KEY_FILE_BYTES} bytes");
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = metadata.permissions().mode();
            if mode & 0o077 != 0 {
                bail!(
                    "Dusk signer key file `{path}` permissions must not allow group or world access; found mode {:o}",
                    mode & 0o777
                );
            }
        }

        let mut reader = file.take(MAX_DUSK_SIGNER_KEY_FILE_BYTES + 1);
        let mut key = String::new();
        reader
            .read_to_string(&mut key)
            .with_context(|| format!("Failed to read Dusk signer key file `{path}`"))?;
        if key.len() as u64 > MAX_DUSK_SIGNER_KEY_FILE_BYTES {
            bail!("Dusk signer key file `{path}` exceeds {MAX_DUSK_SIGNER_KEY_FILE_BYTES} bytes");
        }
        Ok(key)
    }

    fn parse_key_material(key: &str) -> Result<H256, Report> {
        let key = key.trim();
        if key.is_empty() {
            bail!("Dusk signer key material is empty");
        }

        // Dusk signer material is a BLS scalar, not an address. Do not use the
        // shared address parser here: it accepts 20-byte EVM values, Tron
        // addresses, and short bech32 values by left-padding them to H256.
        let bytes = if let Some(hex_key) = key.strip_prefix("0x").or_else(|| key.strip_prefix("0X"))
        {
            hex::decode(hex_key).context("Invalid hexadecimal Dusk signer key")?
        } else if key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            hex::decode(key).context("Invalid hexadecimal Dusk signer key")?
        } else if let Ok((_, decoded)) = bech32::decode(key) {
            decoded
        } else {
            bs58::decode(key)
                .into_vec()
                .context("Invalid base58 Dusk signer key")?
        };

        let key: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            eyre::eyre!(
                "Dusk signer key must decode to exactly 32 bytes, got {}",
                bytes.len()
            )
        })?;
        if key == [0u8; 32] {
            bail!("Dusk signer key must be nonzero");
        }
        Ok(H256::from(key))
    }
}

/// Signer types
#[derive(Default, Debug, Clone)]
pub enum SignerConf {
    /// A local hex key
    HexKey {
        /// Private key value
        key: H256,
    },
    /// An AWS signer. Note that AWS credentials must be inserted into the env
    /// separately.
    Aws {
        /// The UUID identifying the AWS KMS Key
        id: String,
        /// The AWS region
        region: String,
    },
    /// Cosmos Specific key
    CosmosKey {
        /// Private key value
        key: H256,
        /// Prefix for cosmos address
        prefix: String,
        /// Account address type for cosmos address
        account_address_type: AccountAddressType,
    },
    /// Radix Specific key
    RadixKey {
        /// private key
        key: H256,
        /// suffix for address formatting
        suffix: String,
    },
    /// Starknet Specific key
    StarkKey {
        /// Private key value
        key: H256,
        /// Starknet address
        address: H256,
        /// Whether the Starknet signer is legacy
        is_legacy: bool,
    },
    /// Dusk Specific key
    DuskKey {
        /// Private key source
        key: DuskSignerKeyConf,
    },
    /// Assume node will sign on RPC calls
    #[default]
    Node,
}

impl SignerConf {
    /// Try to convert the ethereum signer to a local wallet
    #[instrument(err)]
    pub async fn build<S: BuildableWithSignerConf>(&self) -> Result<S, Report> {
        S::build(self).await
    }
}

/// A signer for a chain.
pub trait ChainSigner: Send {
    /// The address of the signer, formatted in the chain's own address format.
    fn address_string(&self) -> String;
    /// The address of the signer, in h256 format
    fn address_h256(&self) -> H256;
}

/// Builder trait for signers
#[async_trait]
pub trait BuildableWithSignerConf: Sized + ChainSigner {
    /// Build a signer from a conf
    async fn build(conf: &SignerConf) -> Result<Self, Report>;
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_ethereum::Signers {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        Ok(match conf {
            SignerConf::HexKey { key } => hyperlane_ethereum::Signers::Local(LocalWallet::from(
                ethers::core::k256::ecdsa::SigningKey::from(
                    ethers::core::k256::SecretKey::from_be_bytes(key.as_bytes())
                        .context("Invalid ethereum signer key")?,
                ),
            )),
            SignerConf::Aws { id, region } => {
                hyperlane_ethereum::Signers::Aws(build_aws_signer(id, region).await?)
            }
            SignerConf::CosmosKey { .. } => {
                bail!("cosmosKey signer is not supported by Ethereum")
            }
            SignerConf::StarkKey { .. } => {
                bail!("starkKey signer is not supported by Ethereum")
            }
            SignerConf::Node => bail!("Node signer"),
            SignerConf::RadixKey { .. } => {
                bail!("radixKey signer is not supported by Ethereum")
            }
            SignerConf::DuskKey { .. } => {
                bail!("duskKey signer is not supported by Ethereum")
            }
        })
    }
}

impl ChainSigner for hyperlane_ethereum::Signers {
    fn address_string(&self) -> String {
        ethers::signers::Signer::address(self).encode_hex()
    }
    fn address_h256(&self) -> H256 {
        ethers::types::H256::from(ethers::signers::Signer::address(self)).into()
    }
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_tron::TronSigner {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        match conf {
            SignerConf::HexKey { key } => {
                let key = ethers::core::k256::SecretKey::from_be_bytes(key.as_bytes())?;
                let wallet = ethers::core::k256::ecdsa::SigningKey::from(key);
                Ok(hyperlane_tron::TronSigner::from(wallet))
            }
            SignerConf::Aws { id, region } => Ok(hyperlane_tron::TronSigner::Aws(
                build_aws_signer(id, region).await?,
            )),
            _ => bail!(format!("{conf:?} key is not supported by tron")),
        }
    }
}

impl ChainSigner for hyperlane_tron::TronSigner {
    fn address_string(&self) -> String {
        let mut address_bytes = self.address_h256().to_fixed_bytes().to_vec();
        address_bytes[11] = 0x41; // Tron address prefix

        let hash1 = Sha256::digest(&address_bytes[11..]);
        let hash2 = Sha256::digest(hash1);

        let checksum = &hash2[0..4];

        let mut final_bytes = Vec::with_capacity(25);
        final_bytes.extend_from_slice(&address_bytes[11..]);
        final_bytes.extend_from_slice(checksum);

        bs58::encode(final_bytes).into_string()
    }
    fn address_h256(&self) -> H256 {
        ethers::types::H256::from(self.address()).into()
    }
}

#[async_trait]
impl BuildableWithSignerConf for fuels::prelude::WalletUnlocked {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::HexKey { key } = conf {
            let key = fuels::crypto::SecretKey::try_from(key.as_bytes())
                .context("Invalid fuel signer key")?;
            Ok(fuels::prelude::WalletUnlocked::new_from_private_key(
                key, None,
            ))
        } else {
            bail!(format!("{conf:?} key is not supported by fuel"));
        }
    }
}

impl ChainSigner for fuels::prelude::WalletUnlocked {
    fn address_string(&self) -> String {
        self.address().to_string()
    }
    fn address_h256(&self) -> H256 {
        H256::from_slice(fuels::types::Address::from(self.address()).as_slice())
    }
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_sealevel::Keypair {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::HexKey { key } = conf {
            hyperlane_sealevel::create_keypair(key)
        } else {
            bail!(format!("{conf:?} key is not supported by sealevel"));
        }
    }
}

impl ChainSigner for hyperlane_sealevel::Keypair {
    fn address_string(&self) -> String {
        solana_sdk::signer::Signer::pubkey(self).to_string()
    }
    fn address_h256(&self) -> H256 {
        H256::from_slice(&solana_sdk::signer::Signer::pubkey(self).to_bytes())
    }
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_cosmos::Signer {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::CosmosKey {
            key,
            prefix,
            account_address_type,
        } = conf
        {
            Ok(hyperlane_cosmos::Signer::new(
                key.as_bytes().to_vec(),
                prefix.clone(),
                account_address_type,
            )?)
        } else {
            bail!(format!("{conf:?} key is not supported by cosmos"));
        }
    }
}

impl ChainSigner for hyperlane_cosmos::Signer {
    fn address_string(&self) -> String {
        self.address_string.clone()
    }
    fn address_h256(&self) -> H256 {
        self.address_h256()
    }
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_starknet::Signer {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::StarkKey {
            key,
            address,
            is_legacy,
        } = conf
        {
            Ok(hyperlane_starknet::Signer::new(key, address, *is_legacy)?)
        } else {
            bail!(format!("{conf:?} key is not supported by starknet"));
        }
    }
}

impl ChainSigner for hyperlane_starknet::Signer {
    fn address_string(&self) -> String {
        self.address.to_hex_string()
    }

    fn address_h256(&self) -> H256 {
        self.address_h256
    }
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_radix::RadixSigner {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::RadixKey { key, suffix } = conf {
            Ok(hyperlane_radix::RadixSigner::new(
                key.as_bytes().to_vec(),
                suffix.to_string(),
            )?)
        } else {
            bail!(format!("{conf:?} key is not supported by radix"));
        }
    }
}

impl ChainSigner for hyperlane_radix::RadixSigner {
    fn address_string(&self) -> String {
        self.encoded_address.clone()
    }

    fn address_h256(&self) -> H256 {
        self.address_256
    }
}

#[cfg(feature = "aleo")]
#[async_trait]
impl BuildableWithSignerConf for hyperlane_aleo::AleoSigner {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::HexKey { key } = conf {
            Ok(hyperlane_aleo::AleoSigner::new(key.as_bytes())?)
        } else {
            bail!(format!("{conf:?} key is not supported by aleo"));
        }
    }
}

#[cfg(feature = "aleo")]
impl ChainSigner for hyperlane_aleo::AleoSigner {
    fn address_string(&self) -> String {
        self.address().to_owned()
    }

    fn address_h256(&self) -> H256 {
        self.address_h256()
    }
}

#[async_trait]
impl BuildableWithSignerConf for hyperlane_dusk::DuskSigner {
    async fn build(conf: &SignerConf) -> Result<Self, Report> {
        if let SignerConf::DuskKey { key } = conf {
            let key = key.resolve()?;
            hyperlane_dusk::DuskSigner::new(key).map_err(|e| eyre::eyre!(e.to_string()))
        } else {
            bail!(format!("{conf:?} key is not supported by dusk"));
        }
    }
}

impl ChainSigner for hyperlane_dusk::DuskSigner {
    fn address_string(&self) -> String {
        self.address_string()
    }

    fn address_h256(&self) -> H256 {
        self.eth_address()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use ethers::{signers::LocalWallet, utils::hex};
    use hyperlane_core::{AccountAddressType, Encode, H256};
    use moka::future::Cache;
    use rusoto_core::Region;
    use tokio::sync::Barrier;

    use super::resolve_kms_region;
    use crate::settings::{ChainSigner, SignerConf};

    #[test]
    fn dusk_key_decoder_rejects_address_padding_and_invalid_scalars() {
        use crate::settings::signers::DuskSignerKeyConf;

        let valid = DuskSignerKeyConf::Inline {
            key: format!("0x{}", "11".repeat(32)),
        }
        .resolve()
        .expect("32-byte key must parse");
        hyperlane_dusk::DuskSigner::new(valid).expect("canonical nonzero scalar must build");

        for invalid in [
            format!("0x{}", "11".repeat(20)),
            bs58::encode([0x41u8; 25]).into_string(),
            format!("0x{}", "00".repeat(32)),
        ] {
            assert!(DuskSignerKeyConf::Inline { key: invalid }
                .resolve()
                .is_err());
        }

        let noncanonical = DuskSignerKeyConf::Inline {
            key: format!("0x{}", "ff".repeat(32)),
        }
        .resolve()
        .expect("length validation should pass");
        assert!(hyperlane_dusk::DuskSigner::new(noncanonical).is_err());
    }

    /// Exercises the exact coalescing mechanism `build_aws_signer` relies on
    /// (`moka::future::Cache::try_get_with`) against a fake, always-failing initializer -
    /// proving concurrent callers on the same key share one attempt and one failure, and
    /// that a later call is not permanently blocked by a stale cached error.
    #[tokio::test]
    async fn concurrent_failing_lookups_are_coalesced_into_one_attempt() {
        let cache: Cache<&'static str, u32> = Cache::builder().max_capacity(10).build();
        let attempts = Arc::new(AtomicUsize::new(0));

        const WAITERS: usize = 5;
        let barrier = Arc::new(Barrier::new(WAITERS));
        let tasks: Vec<_> = (0..WAITERS)
            .map(|_| {
                let cache = cache.clone();
                let attempts = attempts.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    cache
                        .try_get_with("key", async {
                            attempts.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            Err::<u32, &'static str>("simulated KMS failure")
                        })
                        .await
                })
            })
            .collect();

        for task in tasks {
            let result = task.await.expect("spawned task must not panic");
            assert!(
                result.is_err(),
                "the shared failure must propagate to every waiter"
            );
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "concurrent callers on the same key must coalesce into a single init attempt, \
             not one attempt per waiter"
        );

        // A later, non-concurrent call must retry rather than reuse the (uncached) failure.
        let retry_result = cache
            .try_get_with("key", async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok::<u32, &'static str>(42)
            })
            .await;
        assert_eq!(retry_result, Ok(42));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            2,
            "a later call must retry after a failure, not reuse a permanently cached error"
        );
    }

    #[test]
    fn resolve_kms_region_known_region_uses_enum_variant() {
        assert_eq!(resolve_kms_region("us-east-1"), Region::UsEast1);
    }

    #[test]
    fn resolve_kms_region_unknown_region_uses_custom() {
        let region = resolve_kms_region("eu-central-2");
        match region {
            Region::Custom { name, endpoint } => {
                assert_eq!(name, "eu-central-2");
                assert_eq!(endpoint, "https://kms.eu-central-2.amazonaws.com");
            }
            other => panic!("expected Region::Custom, got {other:?}"),
        }
    }

    #[test]
    fn address_h256_ethereum() {
        const PRIVATE_KEY: &str =
            "2bcd4cb33dc9b879d74aebb847b0fdd27868ade2b3a999988debcaae763283c6";
        const ADDRESS: &str = "0000000000000000000000000bec35c9af305b1b8849d652f4b542d19ef7e8f9";

        let wallet = PRIVATE_KEY
            .parse::<LocalWallet>()
            .expect("Failed to parse private key");

        let chain_signer = hyperlane_ethereum::Signers::Local(wallet);

        let address_h256 = H256::from_slice(
            hex::decode(ADDRESS)
                .expect("Failed to decode public key")
                .as_slice(),
        );
        assert_eq!(chain_signer.address_h256(), address_h256);
    }

    #[test]
    fn address_h256_sealevel() {
        const PRIVATE_KEY: &str =
            "0d861aa9ee7b09fe0305a649ec9aa0dfede421817dbe995b48964e5a79fc89e50f8ac473c042cdd96a1fc81eac32221188807572521429fb871a856a668502a5";
        const ADDRESS: &str = "0f8ac473c042cdd96a1fc81eac32221188807572521429fb871a856a668502a5";

        let chain_signer = hyperlane_sealevel::Keypair::try_from(
            hex::decode(PRIVATE_KEY)
                .expect("Failed to decode private key")
                .as_slice(),
        )
        .expect("Failed to decode keypair");

        let address_h256 = H256::from_slice(
            hex::decode(ADDRESS)
                .expect("Failed to decode public key")
                .as_slice(),
        );
        assert_eq!(chain_signer.address_h256(), address_h256);
    }

    #[test]
    fn address_h256_fuel() {
        const PRIVATE_KEY: &str =
            "0a83ee2a87f328704512567198ee25578c27c707b26fdf3be9ea8bf8588f3b65";
        const PUBLIC_KEY: &str = "b43425b2256e7dcdd61752808b137b23f4f697cfaf21175ed81d0610ebab5a87";

        let private_key = fuels::crypto::SecretKey::try_from(
            hex::decode(PRIVATE_KEY)
                .expect("Failed to decode private key")
                .as_slice(),
        )
        .expect("Failed to create secret key");

        let chain_signer = fuels::prelude::WalletUnlocked::new_from_private_key(private_key, None);

        let address_h256 = H256::from_slice(
            hex::decode(PUBLIC_KEY)
                .expect("Failed to decode public key")
                .as_slice(),
        );
        assert_eq!(chain_signer.address_h256(), address_h256);
    }

    #[test]
    fn address_h256_cosmos() {
        const PRIVATE_KEY: &str =
            "5486418967eabc770b0fcb995f7ef6d9a72f7fc195531ef76c5109f44f51af26";
        const ADDRESS: &str = "000000000000000000000000b5a79b48c87e7a37bdb625096140ee7054816942";

        let key = H256::from_slice(
            hex::decode(PRIVATE_KEY)
                .expect("Failed to decode public key")
                .as_slice(),
        );
        let chain_signer = hyperlane_cosmos::Signer::new(
            key.to_vec(),
            "neutron".to_string(),
            &AccountAddressType::Bitcoin,
        )
        .expect("Failed to create cosmos signer");

        let address_h256 = H256::from_slice(
            hex::decode(ADDRESS)
                .expect("Failed to decode public key")
                .as_slice(),
        );
        assert_eq!(chain_signer.address_h256(), address_h256);
    }

    #[tokio::test]
    async fn address_h256_tron() {
        use crate::settings::signers::BuildableWithSignerConf;

        const PRIVATE_KEY: &str =
            "b2752a4539917a795c79caaa0e99d8111078574f381ca3f7598c6ff1ea6b6e3c";
        let address =
            hex::decode("000000000000000000000000e304de1cb42ac734b97bd6ae767942e00d751f8a")
                .unwrap();

        let signer_config = SignerConf::HexKey {
            key: H256::from_slice(
                hex::decode(PRIVATE_KEY)
                    .expect("Failed to decode private key")
                    .as_slice(),
            ),
        };

        let tron_signer = hyperlane_tron::TronSigner::build(&signer_config)
            .await
            .expect("Failed to build tron signer");

        assert_eq!(H256::from_slice(&address), tron_signer.address_h256());
        assert_eq!(
            "TWfaDp7My62uVWnxPiohWau4HyanfDG31N",
            tron_signer.address_string()
        );
    }

    #[tokio::test]
    async fn dusk_signer_builds_from_key_file() {
        use crate::settings::signers::{BuildableWithSignerConf, DuskSignerKeyConf};

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let key_path = tempdir.path().join("dusk-signer.key");
        let private_key = format!("0x{}", "11".repeat(32));
        std::fs::write(&key_path, format!("{private_key}\n")).expect("write key file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
                .expect("set key file permissions");
        }

        let signer_config = SignerConf::DuskKey {
            key: DuskSignerKeyConf::File {
                path: key_path.to_string_lossy().to_string(),
            },
        };

        let signer = hyperlane_dusk::DuskSigner::build(&signer_config)
            .await
            .expect("build Dusk signer from key file");

        assert_eq!(
            signer.address_h256(),
            H256::from_slice(
                &hex::decode("45f325943f47c662afbdfc9bd0b48f38b162441d92363746d8582677d7e4ce4a")
                    .expect("decode expected Dusk address")
            )
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dusk_signer_rejects_loose_key_file_permissions() {
        use std::os::unix::fs::PermissionsExt;

        use crate::settings::signers::{BuildableWithSignerConf, DuskSignerKeyConf};

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let key_path = tempdir.path().join("dusk-signer.key");
        std::fs::write(&key_path, "unread-before-permission-check\n").expect("write key file");
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o644))
            .expect("set loose key file permissions");

        let signer_config = SignerConf::DuskKey {
            key: DuskSignerKeyConf::File {
                path: key_path.to_string_lossy().to_string(),
            },
        };

        let err = hyperlane_dusk::DuskSigner::build(&signer_config)
            .await
            .expect_err("loose Dusk signer key file permissions must be rejected");

        assert!(
            err.to_string().contains("permissions"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn dusk_signer_rejects_oversized_key_file() {
        use crate::settings::signers::{
            BuildableWithSignerConf, DuskSignerKeyConf, MAX_DUSK_SIGNER_KEY_FILE_BYTES,
        };

        let tempdir = tempfile::tempdir().expect("create tempdir");
        let key_path = tempdir.path().join("oversized-dusk-signer.key");
        std::fs::write(
            &key_path,
            vec![b'a'; (MAX_DUSK_SIGNER_KEY_FILE_BYTES + 1) as usize],
        )
        .expect("write oversized key file");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
                .expect("set key file permissions");
        }

        let signer_config = SignerConf::DuskKey {
            key: DuskSignerKeyConf::File {
                path: key_path.to_string_lossy().to_string(),
            },
        };
        let err = hyperlane_dusk::DuskSigner::build(&signer_config)
            .await
            .expect_err("oversized Dusk signer key file must be rejected");
        assert!(
            err.to_string().contains("exceeds"),
            "unexpected error: {err:?}"
        );
    }
}
