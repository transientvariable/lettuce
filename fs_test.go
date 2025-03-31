//go:build integration

package lettuce

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/transientvariable/anchor"
	"github.com/transientvariable/fs-go"
	"github.com/transientvariable/log-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	gofs "io/fs"
)

const (
	testDataDir   = "testdata"
	storagePrefix = "dev"
)

// LettuceTestSuite ...
type LettuceTestSuite struct {
	suite.Suite
	files     map[string]gofs.FileInfo
	filePaths []string
	lettuce   fs.FS
}

func NewLettuceTestSuite() *LettuceTestSuite {
	return &LettuceTestSuite{}
}

func (t *LettuceTestSuite) SetupTest() {
	lettuce, err := New()
	if err != nil {
		t.T().Fatal(err)
	}
	t.lettuce = lettuce

	dir, err := os.Getwd()
	if err != nil {
		t.T().Fatal(err)
	}
	dir = filepath.Join(dir, testDataDir)

	log.Info("[lettuce_test:setup]", log.String("test_data_dir", dir))

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

			log.Info("[lettuce_test:setup] writing test file",
				log.String("file_path", filePath),
				log.Int("size", len(b)),
				log.String("source", path))

			if err := t.lettuce.WriteFile(filePath, b, modeCreate); err != nil {
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

	log.Info(fmt.Sprintf("[lettuce_test:setup] file paths:\n%s", anchor.ToJSONFormatted(t.filePaths)))
}

func TestLettuceTestSuite(t *testing.T) {
	suite.Run(t, NewLettuceTestSuite())
}

func (t *LettuceTestSuite) TestFS() {
	log.Info(fmt.Sprintf("[lettuce_test] filePaths: %s\n", anchor.ToJSONFormatted(t.filePaths)))

	assert.NoError(t.T(), fstest.TestFS(t.lettuce, t.filePaths...))
}
