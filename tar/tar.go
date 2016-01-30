package tar

import (
	"bytes"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"

	"archive/tar"

	"github.com/go-gonzo/filter/match"
	"github.com/omeid/gonzo"
	"github.com/omeid/gonzo/context"
)

func strip(depth int, path string) string {
	if path == "" {
		return ""
	}

	path = filepath.Clean(path)
	path = strings.TrimPrefix(path, "/")
	sep := strings.Index(path, "/")
	if sep > -1 {
		path = path[sep:]
	}
	if depth > 1 {
		return strip(depth-1, path)
	}
	return strings.TrimPrefix(path, "/")
}

type Options struct {
	//Strip number of leading components from file names on extraction
	StripComponenets int
	// if you pass a list of patterns, it will only untar files that matche at least
	// one of the provided functions.
	Pluck []string
}

// Untar files from input channel and pass the result to the output channel.
func Untar(opt Options) gonzo.Stage {
	return func(ctx context.Context, in <-chan gonzo.File, out chan<- gonzo.File) error {

		//Check patterns.
		pluck := len(opt.Pluck) > 0
		if pluck {
			err := match.Good(opt.Pluck...)
			if err != nil {
				return err
			}
		}

		for {
			select {
			case file, ok := <-in:
				if !ok {
					return nil
				}

				context.WithValue(ctx, "archive", file.FileInfo().Name()).Debug("Untaring")
				tr := tar.NewReader(file)
				defer file.Close()

				// Iterate through the files in the archive.
				for {
					hdr, err := tr.Next()
					if err == io.EOF {
						// end of tar archive
						break
					}
					if err != nil {
						return err
					}

					name := strip(opt.StripComponenets, hdr.Name)
					if pluck && !match.Any(name, opt.Pluck...) {
						continue
					}

					context.WithValue(ctx, "file", name).Debug("Untaring")

					content := new(bytes.Buffer)
					n, err := content.ReadFrom(tr)
					if err != nil {
						return err
					}
					fs := gonzo.NewFile(ioutil.NopCloser(content), gonzo.FileInfoFrom(hdr.FileInfo()))
					fs.FileInfo().SetName(name)
					fs.FileInfo().SetSize(int64(n))

					out <- fs
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
