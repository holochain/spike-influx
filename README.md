- download / unpack influxd binary for your system from https://github.com/influxdata/influxdb/releases/tag/v2.7.1 into project directory
- download / unpack influx binary for your system from https://github.com/influxdata/influx-cli/releases/tag/v2.7.3 into your project directory
- cargo run
- go to the localhost port printed out in your console (something like `http://127.0.0.1:39119`)
- login with user `hello` password `helloworld`

it should be injecting a metric once per second, you can make a query like the following:

![./influxdb.png]
