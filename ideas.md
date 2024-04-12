To check correctness of unsafe code:
- Miri
- Loom
- Kani
- Verus
- Thread sanitizer ?
- Add loads of debug assertions


To improve code:
- Use #[align(...)] instead of manual padding
