use std::sync::Arc;

const MAX_BUFFER: usize = 4096;

#[derive(Clone)]
struct WriteBuffer(Arc<std::sync::Mutex<Vec<influxdb::WriteQuery>>>);

impl WriteBuffer {
    pub fn new() -> Self {
        Self(Arc::new(std::sync::Mutex::new(Vec::new())))
    }

    pub fn push(&self, query: influxdb::WriteQuery) {
        let mut buffer = self.0.lock().unwrap();
        if buffer.len() >= MAX_BUFFER {
            return;
        }
        buffer.push(query);
    }

    pub fn drain(&self) -> Option<Vec<influxdb::WriteQuery>> {
        let mut buffer = self.0.lock().unwrap();
        if buffer.len() == 0 {
            return None;
        }
        Some(std::mem::take(&mut *buffer))
    }
}

struct InfluxUniMetric<T: std::fmt::Display + Into<influxdb::Type>> {
    buffer: WriteBuffer,
    name: std::borrow::Cow<'static, str>,
    description: Option<std::borrow::Cow<'static, str>>,
    unit: Option<opentelemetry::metrics::Unit>,
    _p: std::marker::PhantomData<T>,
}

impl<T: std::fmt::Display + Into<influxdb::Type>> InfluxUniMetric<T> {
    pub fn new(
        buffer: WriteBuffer,
        name: std::borrow::Cow<'static, str>,
        description: Option<std::borrow::Cow<'static, str>>,
        unit: Option<opentelemetry::metrics::Unit>,
    ) -> Self {
        Self {
            buffer,
            name,
            description,
            unit,
            _p: std::marker::PhantomData,
        }
    }

    fn report(&self, value: T, attributes: &[opentelemetry::KeyValue]) {
        let timestamp = influxdb::Timestamp::Nanoseconds(
            std::time::SystemTime::UNIX_EPOCH
                .elapsed()
                .unwrap()
                .as_nanos(),
        );

        let mut write =
            influxdb::WriteQuery::new(timestamp, self.name.to_string()).add_field("value", value);

        if let Some(description) = &self.description {
            write = write.add_tag("description", description.to_string());
        }

        if let Some(unit) = &self.unit {
            write = write.add_tag("unit", unit.as_str().to_string());
        }

        for kv in attributes {
            write = write.add_tag(kv.key.clone(), kv.value.to_string());
        }

        self.buffer.push(write)
    }
}

impl<T: std::fmt::Display + Into<influxdb::Type>> opentelemetry::metrics::SyncCounter<T>
    for InfluxUniMetric<T>
{
    fn add(&self, value: T, attributes: &[opentelemetry::KeyValue]) {
        self.report(value, attributes)
    }
}

impl<T: std::fmt::Display + Into<influxdb::Type>> opentelemetry::metrics::SyncHistogram<T>
    for InfluxUniMetric<T>
{
    fn record(&self, value: T, attributes: &[opentelemetry::KeyValue]) {
        self.report(value, attributes)
    }
}

struct InfluxInstrumentProvider(WriteBuffer);

impl opentelemetry::metrics::InstrumentProvider for InfluxInstrumentProvider {
    fn u64_counter(
        &self,
        name: std::borrow::Cow<'static, str>,
        description: Option<std::borrow::Cow<'static, str>>,
        unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::Counter<u64>> {
        Ok(opentelemetry::metrics::Counter::new(Arc::new(
            InfluxUniMetric::new(self.0.clone(), name, description, unit),
        )))
    }

    fn f64_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::Counter<f64>> {
        todo!()
    }

    fn u64_observable_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<u64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableCounter<u64>> {
        todo!()
    }

    fn f64_observable_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<f64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableCounter<f64>> {
        todo!()
    }

    fn i64_up_down_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::UpDownCounter<i64>> {
        todo!()
    }

    fn f64_up_down_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::UpDownCounter<f64>> {
        todo!()
    }

    fn i64_observable_up_down_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<i64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableUpDownCounter<i64>> {
        todo!()
    }

    fn f64_observable_up_down_counter(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<f64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableUpDownCounter<f64>> {
        todo!()
    }

    fn u64_observable_gauge(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<u64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableGauge<u64>> {
        todo!()
    }

    fn i64_observable_gauge(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<i64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableGauge<i64>> {
        todo!()
    }

    fn f64_observable_gauge(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
        _callback: Vec<opentelemetry::metrics::Callback<f64>>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::ObservableGauge<f64>> {
        todo!()
    }

    fn f64_histogram(
        &self,
        name: std::borrow::Cow<'static, str>,
        description: Option<std::borrow::Cow<'static, str>>,
        unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::Histogram<f64>> {
        Ok(opentelemetry::metrics::Histogram::new(Arc::new(
            InfluxUniMetric::new(self.0.clone(), name, description, unit),
        )))
    }

    fn u64_histogram(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::Histogram<u64>> {
        todo!()
    }

    fn i64_histogram(
        &self,
        _name: std::borrow::Cow<'static, str>,
        _description: Option<std::borrow::Cow<'static, str>>,
        _unit: Option<opentelemetry::metrics::Unit>,
    ) -> opentelemetry::metrics::Result<opentelemetry::metrics::Histogram<i64>> {
        todo!()
    }

    fn register_callback(
        &self,
        _instruments: &[Arc<dyn std::any::Any>],
        _callbacks: Box<dyn Fn(&dyn opentelemetry::metrics::Observer) + Send + Sync>,
    ) -> opentelemetry::metrics::Result<Box<dyn opentelemetry::metrics::CallbackRegistration>> {
        todo!()
    }
}

struct InfluxMeterProvider(WriteBuffer);

impl InfluxMeterProvider {
    pub fn new(client: influxdb::Client) -> Self {
        let buffer = WriteBuffer::new();

        {
            let buffer = buffer.clone();
            tokio::task::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

                    if let Some(data) = buffer.drain() {
                        client.query(data).await.unwrap();
                    }
                }
            });
        }

        Self(buffer)
    }
}

impl opentelemetry::metrics::MeterProvider for InfluxMeterProvider {
    fn versioned_meter(
        &self,
        _name: impl Into<std::borrow::Cow<'static, str>>,
        _version: Option<impl Into<std::borrow::Cow<'static, str>>>,
        _schema_url: Option<impl Into<std::borrow::Cow<'static, str>>>,
        _attributes: Option<Vec<opentelemetry::KeyValue>>,
    ) -> opentelemetry::metrics::Meter {
        opentelemetry::metrics::Meter::new(Arc::new(InfluxInstrumentProvider(self.0.clone())))
    }
}

macro_rules! cmd_output {
    ($cmd:expr $(,$arg:expr)*) => {async move {
        let mut proc = tokio::process::Command::new($cmd);
        proc.kill_on_drop(true);
        $(
            proc.arg($arg);
        )*
        println!("RUNNING: {proc:?}");
        let output = proc.output().await.expect("command failed");
        (
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    }.await}
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    use opentelemetry::metrics::MeterProvider;

    let _ = tokio::fs::remove_dir_all("./influx-data").await;

    println!("{:?}", cmd_output!("./influxd", "version"));
    println!("{:?}", cmd_output!("./influx", "version"));

    tokio::fs::create_dir_all("./influx-data")
        .await
        .expect("made data dir");

    let port = spawn_influxd().await;
    println!("got port: {port}");

    let host = format!("http://127.0.0.1:{port}");
    println!(
        "{:?}",
        cmd_output!(
            "./influx",
            "setup",
            "--json",
            "--configs-path",
            "./influx-data/configs",
            "--host",
            host,
            "--username",
            "hello",
            "--password",
            "helloworld",
            "--org",
            "hello",
            "--bucket",
            "hello",
            "--retention",
            "72h",
            "--force"
        )
    );

    let data = tokio::fs::read("./influx-data/configs").await.unwrap();
    let data = String::from_utf8_lossy(&data);
    let mut token = data.split("token = \"");
    token.next().unwrap();
    let token = token.next().unwrap();
    let mut token = token.split("\"");
    let token = token.next().unwrap();

    println!("GOT TOKEN: {token}");

    let client =
        influxdb::Client::new(format!("http://127.0.0.1:{port}"), "hello").with_token(token);

    println!("{:?}", client.ping().await);

    opentelemetry::global::set_meter_provider(InfluxMeterProvider::new(client));

    let meter = opentelemetry::global::meter_provider().meter("influx-test");
    let counter = meter
        .u64_counter("test-counter")
        .with_description("test-counter-desc")
        .init();

    let meter = opentelemetry::global::meter_provider().meter("influx-test");
    let histogram = meter
        .f64_histogram("test-histogram")
        .with_description("test-histogram-desc")
        .with_unit(opentelemetry::metrics::Unit::new("ms"))
        .init();

    let mut last_time = std::time::Instant::now();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        counter.add(1, &[opentelemetry::KeyValue::new("k1", "v1")]);

        let val = last_time.elapsed().as_secs_f64();
        histogram.record(val, &[opentelemetry::KeyValue::new("k2", "v2")]);

        last_time = std::time::Instant::now();
    }
}

async fn spawn_influxd() -> u16 {
    let (s, r) = tokio::sync::oneshot::channel();

    let mut s = Some(s);

    tokio::task::spawn(async move {
        use tokio::io::AsyncBufReadExt;

        let mut proc = tokio::process::Command::new("./influxd");
        proc.kill_on_drop(true);
        proc.arg("--engine-path").arg("./influx-data/engine");
        proc.arg("--bolt-path").arg("./influx-data/influxd.bolt");
        proc.arg("--http-bind-address").arg("127.0.0.1:0");
        proc.arg("--metrics-disabled");
        proc.arg("--reporting-disabled");
        proc.stdout(std::process::Stdio::piped());

        println!("RUNNING: {proc:?}");

        let mut child = proc.spawn().expect("command failed");
        let stdout = child.stdout.take().unwrap();
        let mut reader = tokio::io::BufReader::new(stdout).lines();
        while let Some(line) = reader.next_line().await.expect("got line") {
            if line.contains("msg=Listening")
                && line.contains("service=tcp-listener")
                && line.contains("transport=http")
            {
                let mut iter = line.split(" port=");
                iter.next().unwrap();
                let item = iter.next().unwrap();
                let port: u16 = item.parse().unwrap();
                if let Some(s) = s.take() {
                    let _ = s.send(port);
                }
            }
            println!("INFLUXD: {line}");
        }
    });

    r.await.expect("channel closed")
}
