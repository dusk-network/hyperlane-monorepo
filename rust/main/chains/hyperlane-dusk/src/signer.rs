use bls12_381_bls::{PublicKey as BlsPublicKey, SecretKey as BlsSecretKey};
use dusk_bytes::Serializable;
use hyperlane_core::H256;

use crate::HyperlaneDuskError;

/// Dusk signer using BLS secret key for Moonlight transactions.
///
/// Stores the raw key material and provides address derivation.
/// Actual transaction signing requires a running Dusk node context
/// and will be implemented in the Moonlight TX builder.
#[derive(Clone)]
pub struct DuskSigner {
    /// The raw 32-byte key seed.
    key: H256,
    /// The BLS public key bytes used as the Moonlight account identifier.
    public_key: BlsPublicKey,
    /// Cached address as H256.
    address: H256,
}

impl std::fmt::Debug for DuskSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuskSigner")
            .field("address", &self.address)
            .finish()
    }
}

impl DuskSigner {
    /// Create a new DuskSigner from a 32-byte key.
    ///
    /// The key is interpreted as a Moonlight BLS secret key (32 bytes).
    ///
    /// The on-chain sender identity for direct Moonlight transactions is
    /// `keccak256(public_key_bytes)`. This matches `Mailbox::resolve_sender()`
    /// in the Dusk Mailbox contract.
    pub fn new(key: H256) -> Result<Self, HyperlaneDuskError> {
        let key_bytes: [u8; 32] = key.into();
        let sk = BlsSecretKey::from_bytes(&key_bytes).map_err(|e| {
            HyperlaneDuskError::InvalidBlsSecretKey(format!("{e:?}"))
        })?;
        let pk = BlsPublicKey::from(&sk);

        let pk_bytes = pk.to_bytes();
        let address_bytes = hyperlane_dusk_types::message::keccak256(&pk_bytes);
        let address = H256::from_slice(&address_bytes);

        Ok(Self {
            key,
            public_key: pk,
            address,
        })
    }

    /// Get the raw key.
    pub fn key(&self) -> &H256 {
        &self.key
    }

    /// Get the BLS public key for this signer (Moonlight account identifier).
    pub fn public_key(&self) -> &BlsPublicKey {
        &self.public_key
    }

    /// Get the H256 address of this signer.
    pub fn eth_address(&self) -> H256 {
        self.address
    }

    /// Get the hex-encoded address string.
    pub fn address_string(&self) -> String {
        // Rusk's `/on/account:{pk}/status` endpoints identify accounts by bs58-encoded
        // BLS public keys, so we use that as the chain-native address string.
        bs58::encode(self.public_key.to_bytes()).into_string()
    }
}
