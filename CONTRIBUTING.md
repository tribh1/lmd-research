# Contributing

This repository is a thesis prototype. Contributions should preserve the core architecture principles:

1. Do not hardcode table-specific logic in job code.
2. Add table-specific behavior through metadata YAML.
3. Keep one physical job per Lakehouse layer transition.
4. Keep shared transformation/governance semantics in `src/common/`.
5. Do not commit secrets or generated data.
6. Update documentation when changing pipeline behavior.

## Suggested workflow

```bash
git checkout -b feature/<short-name>
python -m compileall src
# run relevant scripts/tests
git add .
git commit -m "Describe change"
```
