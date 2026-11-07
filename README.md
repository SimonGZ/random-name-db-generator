# random-name-db-generator

Quick and dirty Ruby script to convert CSV files into PG database entries for [Ranndy Random Name Generator][ranndy].

[ranndy]: https://github.com/SimonGZ/ranndy

## National first-name data

The bundled CSVs cover 1880–2025 and come from the SSA national archive,
including historical revisions in the 2026 release. The `yobYYYY.txt` files
are stored as `YYYY.csv` without changing their contents or row order.

- [SSA download page](https://www.ssa.gov/oact/babynames/limits.html)
- [Archive snapshot used](https://web.archive.org/web/20260918154125id_/https://www.ssa.gov/oact/babynames/names.zip)
- Archive SHA-256 `cd78e975ed7bb358e018dd62fbe14ced89295e9581c49172ca4eedcb011b3724`
- 2025 file SHA-256 `711d65b5a0ab84596b62dc47a942ac2da3bedb1cb32513ff41039ca41d304196`

The direct SSA download returned HTTP 403 when this update was prepared.
