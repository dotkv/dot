package dot

import (
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"strings"
	"testing"
)

func TestMetricsBuildVersion(t *testing.T) {
	const revision = "605b593ab9c6546e798b5b9aec099615fbfdf97f"
	const pseudo = "v0.0.0-20261022182138-605b593ab9c6"
	vcs := []debug.BuildSetting{
		{Key: "vcs.revision", Value: revision},
		{Key: "vcs.modified", Value: "true"},
	}
	for _, tc := range []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{name: "unavailable", want: "dev"},
		{name: "missing module", info: &debug.BuildInfo{}, want: "dev"},
		{
			name: "main release",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0"}},
			want: "v0.1.0",
		},
		{
			name: "main prerelease",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0-rc.1"}},
			want: "v0.1.0-rc.1",
		},
		{
			name: "main pseudo-version",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: pseudo}},
			want: "dev · 605b593",
		},
		{
			name: "main devel",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "(devel)"}},
			want: "dev",
		},
		{
			name: "main devel with revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "(devel)"}, Settings: vcs[:1]},
			want: "dev · 605b593",
		},
		{
			name: "main dirty release",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0"}, Settings: vcs},
			want: "dev · 605b593 · dirty",
		},
		{
			name: "main dirty pseudo-version without settings",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: pseudo + "+dirty"}},
			want: "dev · 605b593 · dirty",
		},
		{
			name: "main dirty tag without revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath, Version: "v0.1.0+dirty"}},
			want: "dev · dirty",
		},
		{
			name: "short revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: dotModulePath}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}}},
			want: "dev · abc",
		},
		{
			name: "dependency release ignores host version and dirty state",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "v9.0.0"}, Settings: vcs,
				Deps: []*debug.Module{
					{Path: "example.com/other", Version: "v1.0.0"},
					{Path: dotModulePath, Version: "v0.1.0"},
				},
			},
			want: "v0.1.0",
		},
		{
			name: "dependency pseudo-version",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: pseudo}}},
			want: "dev · 605b593",
		},
		{
			name: "pseudo-version after release",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: "v0.1.1-0.20261022182138-605b593ab9c6"}}},
			want: "dev · 605b593",
		},
		{
			name: "pseudo-version after prerelease",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: "v0.1.0-rc.1.0.20261022182138-605b593ab9c6"}}},
			want: "dev · 605b593",
		},
		{
			name: "local replacement ignores original version and host revision",
			info: &debug.BuildInfo{
				Settings: vcs,
				Deps:     []*debug.Module{{Path: dotModulePath, Version: "v0.1.0", Replace: &debug.Module{Path: "../dot"}}},
			},
			want: "dev",
		},
		{
			name: "versioned replacement",
			info: &debug.BuildInfo{Deps: []*debug.Module{{Path: dotModulePath, Version: "v0.1.0", Replace: &debug.Module{Path: "example.com/fork", Version: "v0.2.0"}}}},
			want: "v0.2.0",
		},
		{
			name: "unrelated host revision",
			info: &debug.BuildInfo{Main: debug.Module{Path: "example.com/app"}, Settings: vcs},
			want: "dev",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := metricsBuildVersion(tc.info); got != tc.want {
				t.Fatalf("version = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMetricsIndexVersion(t *testing.T) {
	for _, tc := range []struct{ version, escaped string }{
		{"v0.1.0", "v0.1.0"},
		{"dev · 605b593 · dirty", "dev · 605b593 · dirty"},
		{`<script>alert("version")</script>`, "&lt;script&gt;alert(&#34;version&#34;)&lt;/script&gt;"},
	} {
		t.Run(tc.version, func(t *testing.T) {
			data, err := renderMetricsIndex(tc.version)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `<span class="version">`+tc.escaped+`</span>`) {
				t.Fatalf("dashboard did not render escaped version %q", tc.escaped)
			}
		})
	}
}

func TestMetricsHTTPVersion(t *testing.T) {
	// Serving the version needs neither an open database nor a WebSocket client.
	var db DB
	w := httptest.NewRecorder()
	db.MetricsHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	info, _ := debug.ReadBuildInfo()
	want := `<span class="version">` + metricsBuildVersion(info) + `</span>`
	if !strings.Contains(w.Body.String(), want) {
		t.Fatalf("dashboard did not render %q", want)
	}
}
