use crate::Result;

/// Sequential reader state. Input EOF can precede output completion while a
/// decoder drains buffered items; output EOF and errors are terminal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReadState {
    input_eof: bool,
    finished: bool,
}

impl ReadState {
    /// Whether the upstream input has returned EOF.
    pub fn input_eof(&self) -> bool {
        self.input_eof
    }

    /// Record input EOF without finishing output; buffered items may remain.
    pub fn mark_input_eof(&mut self) {
        self.input_eof = true;
    }

    /// Whether output is terminal. Readers must return EOF without further work.
    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// Mark output terminal without changing input EOF.
    pub fn finish(&mut self) {
        self.finished = true;
    }

    /// Mark output terminal on EOF or error.
    pub fn observe<T>(&mut self, result: &Result<Option<T>>) {
        if !matches!(result, Ok(Some(_))) {
            self.finish();
        }
    }
}
