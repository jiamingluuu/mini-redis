use bytes::Bytes;

use crate::types::{hash::Hash, list::List};

/// A Redis value — the type stored in the key-value store.
///
/// REDIS: Every key in the Redis keyspace maps to a robj (Redis object) that
/// carries both the type (OBJ_STRING / OBJ_LIST / OBJ_HASH / …) and a
/// pointer to the encoding-specific data. See server.h robj.
///
/// We encode the type directly in the enum variant, which lets the compiler
/// enforce type safety at no runtime cost.
#[derive(Debug)]
pub(crate) enum RedisObject {
    Str(Bytes),
    Hash(Hash),
    List(List),
}

impl RedisObject {
    /// The type name returned by the Redis TYPE command.
    // Not yet wired to a command handler; will be used when TYPE is implemented.
    #[allow(dead_code)]
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            RedisObject::Str(_) => "string",
            RedisObject::Hash(_) => "hash",
            RedisObject::List(_) => "list",
        }
    }
}
