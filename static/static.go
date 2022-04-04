package static

import (
	"embed"
	"io/fs"
	"net/http"
	"os"

	"github.com/golang/glog"
)

//go:embed *
var fromBuiltin embed.FS

var fromFilesystem = os.DirFS("./static")

func Handler(dev bool) http.Handler {
	var files fs.FS = fromBuiltin
	if dev {
		glog.V(1).Infof("Using assets from filesystem")
		files = fromFilesystem
	}
	return http.StripPrefix("/static", http.FileServer(http.FS(files)))
}
