pub mod commands;
pub mod homie;
pub mod inverter;
pub mod modbus;

/// An error with its sources, as `error: source: source`. Logged as a plain `dyn Error`, journald
/// would only get the error's own message.
pub struct Chain<'a>(pub &'a dyn std::error::Error);

impl std::fmt::Display for Chain<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(f, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}
