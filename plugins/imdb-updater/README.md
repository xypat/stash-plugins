# IMDb Updater

Stash external plugin for scenes inside `<title> [imdbid=ttXXXXXXX]/` folders, for example
`IMDB/Movies/Alien: Romulus (2024) [imdbid=tt18412256]/`. The folders above the title do not matter.

## What it does

- Finds the group of the IMDb ID (whether or not its URL ends with a slash). When it does not exist,
  the plugin creates it and fills it with Stash's own `IMDB` scraper (`scrapeGroupURL`).
  Existing groups are never modified. The plugin creates no other groups: there are no parent
  groups for the root or category folders.
- Tags a new group with the subtype tag under the `__GROUP__` root tag that is named or aliased
  `IMDb`, so give your subtype that alias. Nothing else is set: when the group is created, the
  extended-attributes plugin adds the attribute defaults of that subtype. When there is no such
  tag, a warning is logged and the group is created without it.
- Links the scene to that group.
- For TV episodes (`S01E02`, `1x02`) it also:
  - sets `scene_index` as the running number over the seasons that exist in your library;
  - writes the episode IMDb URL and fills title, date, details, director and cover with the
    `IMDB` scraper (`scrapeSceneURL`).
- Scenes that are already complete are skipped, so the task is safe to run repeatedly.

## Tasks and hooks

- `Sync All`: process every scene under an `[imdbid=...]` folder.
- `Dry Run`: show what would change without writing anything.
- `Auto Sync After Scan`: runs on `Scene.Create.Post` and processes the whole IMDb entry of the new
  scene, so `scene_index` stays consistent when a new season arrives.

A scan runs the hook for several files at once, so runs that have work to do are serialized with a
lock file (`<stash config dir>/cache/imdb-updater.lock`). Without it, files of the same title could
each find no group and each create their own. Scenes outside `[imdbid=...]` folders return before
taking the lock.

## Episode IDs

The `IMDB` scraper cannot look up the IMDb ID of a given episode, and IMDb's episode pages are
behind a bot challenge. The plugin therefore reads IMDb's official non-commercial dataset
`title.episode.tsv.gz` (about 55 MB) and caches the episodes of each series for 24 hours in
`<stash config dir>/cache/imdb-updater-episodes.json`. A cache miss costs one download of the
dataset, which takes around 10 to 20 seconds.

## Requirements

- Stash external plugins enabled, with `python3` available as in the official Docker image.
- The community `IMDB` scraper installed and working (it needs FlareSolverr for the IMDb bot
  challenge, see the scraper's own notes).

## Notes

- Episodes missing from the IMDb dataset (for example brand new ones) are reported as warnings and
  picked up by the next run after the dataset is updated.
