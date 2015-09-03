package zip

import (
	"bytes"
	"io/ioutil"

	"archive/zip"

	"github.com/omeid/gonzo"
	"github.com/omeid/gonzo/context"
)

// Unzip the zip files from input channel and pass the result
// to the output channel.
func Unzip() gonzo.Stage {
	return func(ctx context.Context, in <-chan gonzo.File, out chan<- gonzo.File) error {

		for {
			select {
			case file, ok := <-in:
				if !ok {
					return nil
				}

				raw, err := ioutil.ReadAll(file)
				if err != nil {
					return err
				}
				file.Close()

				r, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
				if err != nil {
					return err
				}

				//counter := c.Counter("unzipping", len(r.File))

				// Iterate through the files in the archive,
				for _, f := range r.File {
					ctx = context.WithValue(ctx, "file", f.Name)
					ctx.Info("Unziping")
					//counter.Set(i+1, f.Name)

					content, err := f.Open()
					if err != nil {
					}
					fs := gonzo.NewFile(content, gonzo.FileInfoFrom(f.FileInfo()))
					fs.FileInfo().SetName(f.Name)

					out <- fs
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
