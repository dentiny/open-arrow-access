use crate::Result;

/// Format-independent lifecycle for a sequential reader.
///
/// Input EOF and output completion are separate: a decoder may still emit
/// buffered items after its input ends. Errors and output EOF are terminal.
/// This helper tracks state only; readers still own their input, buffers,
/// decoder, metadata and any resources that need releasing at completion.
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

    /// Finish output explicitly, including when emitting a known final item.
    /// Does not imply that the upstream input reached EOF.
    pub fn finish(&mut self) {
        self.finished = true;
    }

    /// Record an output result. An item preserves the current state; EOF or an
    /// error finishes output permanently. Callers return the result unchanged.
    pub fn observe<T>(&mut self, result: &Result<Option<T>>) {
        if !matches!(result, Ok(Some(_))) {
            self.finish();
        }
    }
}
