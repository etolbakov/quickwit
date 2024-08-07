// Copyright (C) 2024 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::fmt;
use std::time::Duration;

pub struct PrettySample<I>(I, usize);

impl<I> PrettySample<I> {
    pub fn new(slice: I, sample_size: usize) -> Self {
        Self(slice, sample_size)
    }
}

impl<I, T> fmt::Debug for PrettySample<I>
where
    I: IntoIterator<Item = T> + Clone,
    T: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[")?;
        // in general we will get passed a reference (&[...], &HashMap...) or a Map<_> of them.
        // So we either perform a Copy, or a cheap Clone of a simple struct
        let mut iter = self.0.clone().into_iter().enumerate();
        for (i, item) in &mut iter {
            if i > 0 {
                write!(formatter, ", ")?;
            }
            write!(formatter, "{item:?}")?;
            if i == self.1 - 1 {
                break;
            }
        }
        let left = iter.count();
        if left > 0 {
            write!(formatter, ", and {left} more")?;
        }
        write!(formatter, "]")?;
        Ok(())
    }
}

pub trait PrettyDisplay {
    fn pretty_display(&self) -> impl fmt::Display;
}

struct PrettyDurationDisplay<'a>(&'a Duration);

impl fmt::Display for PrettyDurationDisplay<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        // This is enough for my current use cases. To be extended as you see fit.
        let duration_millis = self.0.as_millis();

        if duration_millis < 1_000 {
            return write!(formatter, "{}ms", duration_millis);
        }
        write!(
            formatter,
            "{}.{}s",
            duration_millis / 1_000,
            duration_millis % 1_000 / 10
        )
    }
}

impl PrettyDisplay for Duration {
    fn pretty_display(&self) -> impl fmt::Display {
        PrettyDurationDisplay(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pretty_sample() {
        let pretty_sample = PrettySample::<&[usize]>::new(&[], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[]");

        let pretty_sample = PrettySample::new(&[1], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1]");

        let pretty_sample = PrettySample::new(&[1, 2], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1, 2]");

        let pretty_sample = PrettySample::new(&[1, 2, 3], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1, 2, and 1 more]");

        let pretty_sample = PrettySample::new(&[1, 2, 3, 4], 2);
        assert_eq!(format!("{pretty_sample:?}"), "[1, 2, and 2 more]");
    }

    #[test]
    fn test_pretty_duration() {
        let pretty_duration = Duration::from_millis(0);
        assert_eq!(format!("{}", pretty_duration.pretty_display()), "0ms");

        let pretty_duration = Duration::from_millis(125);
        assert_eq!(format!("{}", pretty_duration.pretty_display()), "125ms");

        let pretty_duration = Duration::from_millis(1_000);
        assert_eq!(format!("{}", pretty_duration.pretty_display()), "1.0s");

        let pretty_duration = Duration::from_millis(1_125);
        assert_eq!(format!("{}", pretty_duration.pretty_display()), "1.12s");
    }
}
