# pmc-oa-watch
Track alterations to text for papers within PubMed Central's Open Access Subset.

To view diffs.

1. Clone this repository
2. Run `git config diff.lfs.textconv cat` to show the raw contents diff of the `hashes.tsv` file rather than the diff of its SHA hash (Git LFS default behavior).
3. Run `git diff <commit1> <commit2>` where `<commit1>` is the more recent commit. PMC IDs with only a `+` entry are new open access manuscripts. PMC IDs with both a `+` and `-` entry would indicate possible editing of the raw PMC text stored within the OA repository.
