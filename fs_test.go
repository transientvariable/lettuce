//go:build integration

package seaweedfs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/transientvariable/fs"
	"github.com/transientvariable/log"
	"github.com/transientvariable/support"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	gofs "io/fs"
)

const (
	testDataDir   = "testdata"
	storagePrefix = "dev"
)

// SeaweedFSTestSuite ...
type SeaweedFSTestSuite struct {
	suite.Suite
	files     map[string]gofs.FileInfo
	filePaths []string
	seaweedfs fs.FS
}

func NewSeaweedFSTestSuite() *SeaweedFSTestSuite {
	return &SeaweedFSTestSuite{}
}

func (t *SeaweedFSTestSuite) SetupTest() {
	seaweedfs, err := New()
	if err != nil {
		t.T().Fatal(err)
	}
	t.seaweedfs = seaweedfs

	dir, err := os.Getwd()
	if err != nil {
		t.T().Fatal(err)
	}
	dir = filepath.Join(dir, testDataDir)

	log.Info("[seaweedfs_test]", log.String("test_data_dir", dir))

	t.files = make(map[string]gofs.FileInfo)
	err = filepath.Walk(dir, func(path string, fi gofs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !fi.IsDir() && fi.Name() != "." {
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			filePath := filepath.Join(storagePrefix, strings.TrimPrefix(path, dir+"/"))

			log.Info("[seaweedfs_test] writing test file",
				log.String("file_path", filePath),
				log.Int("size", len(b)),
				log.String("source", path))

			if err := t.seaweedfs.WriteFile(filePath, b, modeCreate); err != nil {
				return err
			}
			t.files[filePath] = fi
		}
		return nil
	})
	if err != nil {
		t.T().Fatal(err)
	}

	var filePaths []string
	for p := range t.files {
		filePaths = append(filePaths, p)
	}
	t.filePaths = filePaths

	log.Info(fmt.Sprintf("[seaweedfs_test:setup] file paths:\n%s", support.ToJSONFormatted(t.filePaths)))
}

func TestSeaweedFSTestSuite(t *testing.T) {
	suite.Run(t, NewSeaweedFSTestSuite())
}

func (t *SeaweedFSTestSuite) TestFS() {
	log.Info(fmt.Sprintf("[seaweedfs_test] filePaths: %s\n", support.ToJSONFormatted(t.filePaths)))

	assert.NoError(t.T(), fstest.TestFS(t.seaweedfs, t.filePaths...))
}
