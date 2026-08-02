//! Multi-tenant namespace isolation (T11-C).
//!
//! Provides namespace-level key isolation by mixing a tenant_id 
//! into the XXH64 hash seed. Different tenants' keys map to 
//! different hash table slots with high probability.
//!
//! # Design
//!
//! - **Not** physical MR isolation (too expensive for this phase)
//! - Uses xxhash seed mixing: `hash = xxh64(key, tenant_id as u64)`
//! - Same physical CuckooTable, different logical namespaces

use xxhash_rust::xxh64::xxh64;

/// A tenant namespace that provides isolated key hashing.
/// Two tenants with different IDs will hash the same key to different slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TenantNamespace {
    tenant_id: u64,
}

impl TenantNamespace {
    /// Create a new tenant namespace.
    pub fn new(tenant_id: u64) -> Self {
        Self { tenant_id }
    }

    /// Get the tenant ID.
    pub fn id(&self) -> u64 {
        self.tenant_id
    }

    /// Hash a key with this tenant's namespace.
    /// Uses XXH64 with tenant_id as seed, ensuring different tenants
    /// produce different hashes for the same key.
    pub fn hash_key(&self, key: &[u8]) -> u64 {
        xxh64(key, self.tenant_id)
    }

    /// Produce a HashedKey for this tenant's namespace.
    /// Uses XXH64 with tenant_id as seed for the primary hash,
    /// and tenant_id XOR 0xFFFFFFFFFFFF as seed for the digest.
    pub fn hashed_key(&self, key: &[u8]) -> crate::engine::layout::HashedKey {
        let hash = xxh64(key, self.tenant_id);
        let digest_seed = self.tenant_id ^ 0xFFFF_FFFF_FFFF_FFFF;
        let digest_hash = xxh64(key, digest_seed);
        let mut digest = [0u8; 16];
        digest[0..8].copy_from_slice(&hash.to_le_bytes());
        digest[8..16].copy_from_slice(&digest_hash.to_le_bytes());
        crate::engine::layout::HashedKey { hash, digest }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_different_tenants_produce_different_hashes() {
        let t1 = TenantNamespace::new(1);
        let t2 = TenantNamespace::new(2);
        let key = b"test_key";
        assert_ne!(t1.hash_key(key), t2.hash_key(key));
    }

    #[test]
    fn test_same_tenant_produces_same_hash() {
        let t1 = TenantNamespace::new(42);
        let t2 = TenantNamespace::new(42);
        let key = b"consistent";
        assert_eq!(t1.hash_key(key), t2.hash_key(key));
    }

    #[test]
    fn test_hashed_key_contains_digest() {
        let tenant = TenantNamespace::new(100);
        let hk = tenant.hashed_key(b"hello");
        assert!(hk.hash > 0);
        // Digest should be non-zero
        assert!(hk.digest.iter().any(|&b| b != 0));
    }
}
