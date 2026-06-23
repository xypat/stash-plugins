# Compress Performer Covers

Compresses performer covers that exceed a configured size to AVIF and writes them back through
the `performerUpdate` mutation.

Default behavior:

- Target size: 100 KiB
- Maximum width: 720 px
- Minimum quality: 60
- Concurrency: 3
- Covers at or below the target size are skipped
- Quality is prioritized; when quality would fall below 75, dimensions are reduced gradually
- The minimum dimension is 160 px

## Runtime requirements

The plugin uses only the Python standard library. AVIF encoding is performed by the FFmpeg and
FFprobe binaries configured in Stash.

- The official Docker image includes Python 3, FFmpeg, and FFprobe and is the primary supported
  environment.
- Windows, macOS, and custom containers must provide a working `python3` command.
- FFmpeg must include the `libaom-av1` encoder and the `avif` muxer. The plugin validates both
  before processing covers.
- The default binary paths are read from GraphQL `systemStatus`.
- Use `FFmpeg path override` in the plugin settings if automatic path detection does not work.

The plugin does not bundle platform-specific binaries and does not depend on Pillow, pyvips, or
sharp. This avoids maintaining native dependencies for each operating system and CPU architecture.

## Usage

1. Adjust settings under `Settings -> Plugins -> Installed Plugins` if needed.
2. Run `Preview compression` from `Settings -> Tasks -> Plugin Tasks`.
3. Review the compression results in the task log.
4. Run `Compress performer covers` to write the compressed covers to Stash.

Set `Performer limit` to `0` to process all performers. The preview task performs real AVIF
encoding but does not call the update mutation, so it can also verify the local FFmpeg build and
the configured parameters.

## Automatic compression

The plugin listens for `Performer.Update.Post`. It processes the current performer only when the
update explicitly includes the `image` field. Changes to names, tags, ratings, or other metadata do
not trigger compression.

After compression, the plugin writes the AVIF cover through `performerUpdate`. Stash prevents the
same hook from recursively triggering within its own hook call chain, so this update does not form
an infinite loop. Covers already at or below the target size are skipped.

## Container notes

The plugin process runs inside the Stash container. Do not configure a host-only FFmpeg path; the
path must be visible inside the container. The official image normally requires no path override.
A custom image must include Python 3 and an FFmpeg build with AVIF support.
