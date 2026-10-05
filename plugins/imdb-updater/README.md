# IMDb Updater

Stash external plugin for scenes whose folder name contains `[imdbid=ttXXXXXXX]`, for example
`Alien: Romulus (2024) [imdbid=tt18412256]`.

## What it does

- Finds the group whose URL is `https://www.imdb.com/title/<imdbid>/`. When it does not exist, the
  plugin creates it under `IMDB` -> `电影` (movies) or `电视剧` (TV shows) and fills it with Stash's
  own `IMDB` scraper (`scrapeGroupURL`). Existing groups are never modified, so groups you moved
  to another category (for example `动漫`) stay where they are.
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

- Group names of the root and the two categories are defined in `constants.py`.
- Episodes missing from the IMDb dataset (for example brand new ones) are reported as warnings and
  picked up by the next run after the dataset is updated.
