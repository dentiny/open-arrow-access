use std::collections::HashSet;

/// Plans when to release computed Arrow arrays within one batch.
/// A "value" is an array produced by a generated-column expression or a schema
/// evolution add/cast step, stored in the executor's `values` map under a column
/// slot ID. A cast may replace the array in an existing slot after reading it.
/// Final output values remain available until the output batch is assembled.
pub(crate) struct ValueLifetimePlan {
    /// `releases[step_index]` lists the slot IDs to remove from `values` after
    /// that step executes and validates its result. Removal drops the executor's
    /// array reference; other arrays or output batches may still share its buffers.
    pub(crate) releases: Vec<Vec<usize>>,
}

impl ValueLifetimePlan {
    pub(crate) fn new<'a>(
        steps: impl IntoIterator<Item = (Option<usize>, &'a [usize])>,
        outputs: &[usize],
    ) -> Self {
        let steps: Vec<_> = steps.into_iter().collect();
        let written: HashSet<_> = steps.iter().filter_map(|(output, _)| *output).collect();
        let mut live: HashSet<_> = outputs.iter().copied().collect();
        let mut releases = vec![Vec::new(); steps.len()];
        for (position, (output, inputs)) in steps.into_iter().enumerate().rev() {
            if let Some(output) = output
                && !live.remove(&output)
            {
                releases[position].push(output);
            }
            for &input in inputs {
                if live.insert(input) && written.contains(&input) && Some(input) != output {
                    releases[position].push(input);
                }
            }
        }
        Self { releases }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwrites_release_only_the_value_written_by_the_current_step() {
        // Cast slot 0 twice, copy its current value to 1, then cast 0 again.
        let inputs = [vec![0], vec![0], vec![0], vec![0]];
        let steps = [0, 0, 1, 0];
        let plan = ValueLifetimePlan::new(
            steps
                .into_iter()
                .zip(&inputs)
                .map(|(output, inputs)| (Some(output), inputs.as_slice())),
            &[1],
        );
        assert_eq!(plan.releases, [vec![], vec![], vec![], vec![0]]);
        let plan = ValueLifetimePlan::new(
            steps
                .into_iter()
                .zip(&inputs)
                .map(|(output, inputs)| (Some(output), inputs.as_slice())),
            &[0, 1],
        );
        assert!(plan.releases.iter().all(Vec::is_empty));
    }
}
