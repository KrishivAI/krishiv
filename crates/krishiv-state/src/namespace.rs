/// A state namespace scoped to one operator and one logical state variable.
///
/// The compound name `{operator_id}:{state_name}` is unique per job.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace {
    operator_id: String,
    state_name: String,
}

impl Namespace {
    /// Create a namespace.
    pub fn new(operator_id: impl Into<String>, state_name: impl Into<String>) -> Self {
        Self {
            operator_id: operator_id.into(),
            state_name: state_name.into(),
        }
    }

    /// Operator that owns this namespace.
    pub fn operator_id(&self) -> &str {
        &self.operator_id
    }

    /// Logical state variable name within the operator.
    pub fn state_name(&self) -> &str {
        &self.state_name
    }
}
