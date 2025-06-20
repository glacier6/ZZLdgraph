/*
 * Copyright 2016-2025 Hypermode Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package x

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strings"

	"github.com/golang/glog"

	"github.com/dgraph-io/ristretto/v2/z"
)

var (
	// These variables are set using -ldflags
	dgraphVersion  string
	dgraphCodename string
	gitBranch      string
	lastCommitSHA  string
	lastCommitTime string
)

func init() {
	// check if any version is set by ldflags. If not so, it should be set as "dev"
	if dgraphVersion == "" {
		dgraphVersion = "dev"
	}
}

// BuildDetails returns a string containing details about the Dgraph binary.
func BuildDetails() string {
	licenseInfo := `Licensed under the Apache Public License 2.0`
	if !strings.HasSuffix(dgraphVersion, "-oss") {
		licenseInfo = "Licensed variously under the Apache Public License 2.0 and Dgraph " +
			"Community License"
	}

	buf := z.CallocNoRef(1, "X.BuildDetails")
	jem := len(buf) > 0
	z.Free(buf)

	return fmt.Sprintf(`
Dgraph version   : %v
Dgraph codename  : %v
Dgraph SHA-256   : %x
Commit SHA-1     : %v
Commit timestamp : %v
Branch           : %v
Go version       : %v
jemalloc enabled : %v

For Dgraph official documentation, visit https://dgraph.io/docs.
For discussions about Dgraph     , visit https://discuss.dgraph.io.
For fully-managed Dgraph Cloud   , visit https://dgraph.io/cloud.

%s.
Copyright 2015-2025 Hypermode Inc.

`,
		dgraphVersion, dgraphCodename, ExecutableChecksum(), lastCommitSHA, lastCommitTime, gitBranch,
		runtime.Version(), jem, licenseInfo)
}

// PrintVersion prints version and other helpful information if --version.
func PrintVersion() {
	glog.Infof("\n%s\n", BuildDetails())
}

// Version returns a string containing the dgraphVersion.
func Version() string {
	return dgraphVersion
}

// Codename returns a string containing the dgraphCodename.
func Codename() string {
	return dgraphCodename
}

// pattern for  dev version = min. 7 hex digits of commit-hash.
var versionRe *regexp.Regexp = regexp.MustCompile(`-g[[:xdigit:]]{7,}`)

// DevVersion returns true if the version string contains the above pattern
// e.g.
// 如果版本字符串包含下述，DevVersion将返回true
//  1. v2.0.0-rc1-127-gd20a768b3 => dev version
//  2. v2.0.0 => prod version
func DevVersion() (matched bool) {
	return versionRe.MatchString(dgraphVersion)
}

// ExecutableChecksum returns a byte slice containing the SHA256 checksum of the executable.
// It returns a nil slice if there's an error trying to calculate the checksum.
func ExecutableChecksum() []byte {
	execPath, err := os.Executable()
	if err != nil {
		return nil
	}
	execFile, err := os.Open(execPath)
	if err != nil {
		return nil
	}
	defer func() {
		if err := execFile.Close(); err != nil {
			glog.Warningf("error closing file: %v", err)
		}
	}()

	h := sha256.New()
	if _, err := io.Copy(h, execFile); err != nil {
		return nil
	}

	return h.Sum(nil)
}
