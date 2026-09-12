use zhir_core::error::{Error, Failure};

pub(crate) fn failure(error: &Error) -> Failure {
    match error {
        Error::Model(f) | Error::RuntimeTool(f) => f.clone(),
        Error::Invalid(s) => Failure::new("invalid_arguments", s),
        Error::Validation(_) | Error::Catalog(_) | Error::Context(_) => {
            Failure::new("invalid_arguments", error.to_string())
        }
        Error::Resume(_) => Failure::new("resume", error.to_string()),
        Error::Protocol(s) => Failure::new("protocol", s),
        Error::Deadline => Failure::new("deadline", "operation deadline reached"),
        Error::Cancelled => Failure::new("cancelled", "operation cancelled"),
        _ => Failure::new("infrastructure", error.to_string()),
    }
}
