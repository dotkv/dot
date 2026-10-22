package dot

import (
	"regexp"
	"runtime/debug"
	"strings"
)

const dotModulePath = "github.com/dotkv/dot"

// All Go pseudo-version forms end with a timestamp and a revision identifier.
var metricsPseudoVersion = regexp.MustCompile(`[.-][0-9]{14}-([0-9a-f]{12})$`)

func metricsBuildVersion(info *debug.BuildInfo) string {
	if info == nil {
		return "dev"
	}
	if info.Main.Path == dotModulePath {
		var revision string
		dirty := strings.HasSuffix(info.Main.Version, "+dirty") || strings.HasSuffix(info.Main.Version, ".dirty")
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				revision = setting.Value
			case "vcs.modified":
				dirty = dirty || setting.Value == "true"
			}
		}
		return metricsVersionLabel(info.Main.Version, revision, dirty)
	}
	for _, dep := range info.Deps {
		if dep.Path != dotModulePath {
			continue
		}
		if dep.Replace != nil {
			dep = dep.Replace
		}
		// The host application's VCS metadata does not describe this dependency.
		// A local replacement has no version and must not inherit the original.
		return metricsVersionLabel(dep.Version, "", false)
	}
	return "dev"
}

func metricsVersionLabel(version, revision string, dirty bool) string {
	base, _, _ := strings.Cut(version, "+")
	if match := metricsPseudoVersion.FindStringSubmatch(base); match != nil {
		if revision == "" {
			revision = match[1]
		}
		version = ""
	}
	if version == "" || version == "(devel)" || dirty {
		version = "dev"
		if revision != "" {
			version += " · " + revision[:min(7, len(revision))]
		}
	}
	if dirty {
		version += " · dirty"
	}
	return version
}
