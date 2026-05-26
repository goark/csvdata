# Copilot Instructions for csvdata

## Project Purpose

- `csvdata` provides a small, composable interface to read tabular data as CSV-like rows.
- The package supports plain CSV and adapters for spreadsheet formats:
  - `exceldata` for `.xlsx`
  - `calcdata` for `.ods`

## Design Principles

- Keep the public API small and predictable.
- Keep adapters thin: format-specific logic stays in adapter packages.
- Favor explicit error returns over hidden fallback behavior.
- Preserve backward compatibility unless a breaking change is explicitly planned.

## Error Handling Policy

- Return wrapped errors so callers can still use `errors.Is` and `errors.As`.
- Keep sentinel errors stable when exposed from public API.
- Handle nil-sensitive paths defensively (`io.Reader`, closer, and adapter resources).
- Do not panic for normal runtime failures; return errors with context.

## Testing and Validation

- Primary local check:

```text
task test
```

- Equivalent direct commands:

```text
go mod verify
go test -shuffle on ./...
golangci-lint-v2 run --enable gosec --timeout 3m0s ./...
```

- Prefer deterministic tests. Avoid external network dependencies.

## Lint and Security Notes

- In deferred cleanup, merge close errors with existing return errors using `errs.Join`.
  - Example policy: `err = errs.Join(err, errs.Wrap(closeErr, errs.WithContext(...)))`
- For file path handling, prefer sanitizing with `filepath.Clean` before opening files.
  - Avoid using `#nosec G304` as the first option when path normalization can mitigate the warning.
- For narrowing integer conversions (`G115`), prefer explicit range checks before conversion.
  - If linter still reports a false positive after range checks, allow minimal `#nosec G115` with a reason comment.
- Prefer clear conversions over bit-mask tricks for readability.
  - After range checks, use direct conversion (e.g. `byte(v)`), not masking (e.g. `byte(v & 0xff)`).

## Release Process

- Create releases from `main`.
- Use semantic version tags in `vMAJOR.MINOR.PATCH` format.
- Create annotated tag and push:

```text
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

- Create GitHub release with generated notes:

```text
gh release create vX.Y.Z --generate-notes
```

- Verify module version and release assets after publishing.
