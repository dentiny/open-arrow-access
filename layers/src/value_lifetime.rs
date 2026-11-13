use std::collections::HashSet;

/// Releases computed slots after their last use, including overwritten values.
/// Each step reads its inputs before replacing its optional output slot.
pub(crate) struct ValueLifetimePlan {
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
