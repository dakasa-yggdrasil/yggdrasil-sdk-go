package surface

import (
	"fmt"
	"io/fs"

	"gopkg.in/yaml.v3"
)

// LoadManifestFromBytes parses YAML or JSON into a Manifest and
// validates it. Returns the populated Manifest or an error suitable
// for surfacing to the integration developer at startup.
func LoadManifestFromBytes(raw []byte) (Manifest, error) {
	var m Manifest
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return Manifest{}, fmt.Errorf("surface: parse manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// LoadManifestFromFS reads `path` from `f` (typically an embed.FS) and
// returns the parsed Manifest. Convenience for adapters that embed
// surface/manifest.yaml at compile time.
func LoadManifestFromFS(f fs.FS, path string) (Manifest, error) {
	raw, err := fs.ReadFile(f, path)
	if err != nil {
		return Manifest{}, fmt.Errorf("surface: read %s: %w", path, err)
	}
	return LoadManifestFromBytes(raw)
}
