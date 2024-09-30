//go:build !custom || inputs || inputs.fems

package all

import _ "github.com/influxdata/telegraf/plugins/inputs/fems" // register plugin
