use std::{error::Error, fmt::Display, sync::Arc};

/// A string-based key type used to store user-supplied data.
///
/// We use an [`Arc`]-wrapped [`String`] because keys often get cloned. For bare strings, this
/// would require a reallocation, but with the `Arc` wrapper only reference counter is
/// incremented.
#[derive(Debug, PartialEq, Eq, Hash, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClientKey(Arc<String>);

impl std::ops::Deref for ClientKey {
    type Target = Arc<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ClientKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl From<Arc<String>> for ClientKey {
    fn from(k: Arc<String>) -> Self {
        Self(k)
    }
}

impl From<String> for ClientKey {
    fn from(k: String) -> Self {
        Self::from(Arc::new(k))
    }
}

impl From<&str> for ClientKey {
    fn from(k: &str) -> Self {
        Self::from(k.to_owned())
    }
}

/// Used to signal errors in messages.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum AnnaError {
    /// The requested key does not exist.
    KeyDoesNotExist,
    /// The request was sent to the wrong thread, which is not responsible for the
    /// key.
    WrongThread,
    /// The request timed out.
    Timeout,
    /// The lattice type was not correctly specified or conflicted with an
    /// existing key.
    Lattice,
    /// This error is returned by the routing tier if no servers are in the
    /// cluster.
    NoServers,
    /// Failed to serialize a message.
    Serialize,
}

impl Display for AnnaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyDoesNotExist => write!(f, "The requested key does not exist."),
            Self::WrongThread => write!(
                f,
                "The request was sent to the wrong thread, which is not responsible for the key."
            ),
            Self::Timeout => write!(f, "The request timed out."),

            Self::Lattice => write!(
                f,
                "The lattice type was not correctly specified or conflicted with an existing key."
            ),
            Self::NoServers => write!(
                f,
                "This error is returned by the routing tier if no servers are in the cluster."
            ),
            Self::Serialize => write!(f, "Serialization error."),
        }
    }
}

impl Error for AnnaError {}

impl From<serde_json::Error> for AnnaError {
    fn from(_: serde_json::Error) -> Self {
        Self::Serialize
    }
}
