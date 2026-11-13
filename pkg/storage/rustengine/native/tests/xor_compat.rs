// SPDX-License-Identifier: AGPL-3.0-only

// The XOR writer must produce the bytes of the earlier one, which Prometheus' decoder was checked against.
#[allow(dead_code)]
mod old_xor;

use mimir_engine::xor;

fn rng(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn matches_previous_encoder_and_round_trips() {
    let mut state = 0x1234_5678_9abc_def0u64;
    for case in 0..3000 {
        let n = 1 + (rng(&mut state) % 240) as usize;
        let mut t = (rng(&mut state) % 2_000_000_000_000) as i64 - 1_000_000_000_000;
        let mut v = 0f64;
        let mut samples = Vec::with_capacity(n);
        for _ in 0..n {
            t += match rng(&mut state) % 6 {
                0 => 15_000,
                1 => 15_000 + (rng(&mut state) % 25) as i64,
                2 => (rng(&mut state) % 100_000) as i64,
                3 => (rng(&mut state) % (1 << 21)) as i64,
                4 => (rng(&mut state) % (1 << 40)) as i64,
                _ => 1,
            };
            v = match rng(&mut state) % 7 {
                0 => v,
                1 => v + 1.0,
                2 => f64::from_bits(rng(&mut state)),
                3 => (rng(&mut state) % 1000) as f64 / 100.0,
                4 => f64::NAN,
                5 => f64::from_bits(0x7ff0000000000002),
                _ => v * 1.0001 + 0.5,
            };
            samples.push((t, v));
        }
        let mut new = xor::Appender::default();
        let mut old = old_xor::Appender::default();
        for &(t, v) in &samples {
            new.append(t, v);
            old.append(t, v);
        }
        assert_eq!(new.bytes(), old.bytes(), "case {case}");
        let decoded = xor::decode(new.bytes());
        assert_eq!(decoded.len(), samples.len());
        for (a, b) in decoded.iter().zip(&samples) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1.to_bits(), b.1.to_bits());
        }
    }
}
