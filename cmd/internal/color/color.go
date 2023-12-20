package color

import (
	"os"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
)

// var Active *color.Color = color.New(color.FgGreen, color.Bold)
var Locus *color.Color = color.New(color.FgWhite, color.Bold)
var Warning *color.Color = color.New(color.FgMagenta, color.Bold)
var Error *color.Color = color.New(color.FgRed, color.Bold)
var Fatal *color.Color = color.New(color.FgWhite, color.Bold, color.BgRed)

//var P *color.Color = color.New(color.FgWhite, color.Bold, color.BgBlue)

func AlwaysColor() {
	color.NoColor = false
}

func AutoColor() {
	color.NoColor = os.Getenv("TERM") == "dumb" ||
		(!isatty.IsTerminal(os.Stderr.Fd()) && !isatty.IsCygwinTerminal(os.Stderr.Fd()))
}

func NeverColor() {
	color.NoColor = true
}
