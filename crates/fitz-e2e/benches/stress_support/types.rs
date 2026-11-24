use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Domain {
    Queue,
    Kv,
    Stream,
    Schedule,
    Rpc,
    Notice,
    Lease,
}

impl Domain {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Kv => "kv",
            Self::Stream => "stream",
            Self::Schedule => "schedule",
            Self::Rpc => "rpc",
            Self::Notice => "notice",
            Self::Lease => "lease",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StepOutcome {
    Completed,
    CompletedWithCapacityRejections { code: u32, count: u64 },
    CapacityRejected(u32),
    Contended,
    DeliveryWindowMiss,
}

impl StepOutcome {
    pub(crate) const fn is_completed(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::CompletedWithCapacityRejections { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureKind {
    Timeout,
    InvalidResponse,
    DomainError,
    Transport,
    Verification,
}

#[derive(Debug)]
pub(crate) struct BenchFailure {
    pub(crate) kind: FailureKind,
    pub(crate) detail: String,
}

impl BenchFailure {
    pub(crate) fn domain_error(detail: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::DomainError,
            detail: detail.into(),
        }
    }

    pub(crate) fn validation(detail: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::InvalidResponse,
            detail: detail.into(),
        }
    }

    pub(crate) fn transport(detail: impl fmt::Display) -> Self {
        Self {
            kind: FailureKind::Transport,
            detail: detail.to_string(),
        }
    }

    pub(crate) fn verification(detail: impl Into<String>) -> Self {
        Self {
            kind: FailureKind::Verification,
            detail: detail.into(),
        }
    }
}

impl fmt::Display for BenchFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.detail)
    }
}
