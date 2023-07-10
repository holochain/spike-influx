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
    let _ = tokio::fs::remove_dir_all("./influx-data").await;

    println!("{:?}", cmd_output!("./influxd", "version"));
    println!("{:?}", cmd_output!("./influx", "version"));

    tokio::fs::create_dir_all("./influx-data").await.expect("made data dir");

    let port = spawn_influxd().await;
    println!("got port: {port}");

    let host = format!("http://127.0.0.1:{port}");
    println!("{:?}", cmd_output!(
        "./influx", "setup",
        "--json",
        "--configs-path", "./influx-data/configs",
        "--host", host,
        "--username", "hello",
        "--password", "helloworld",
        "--org", "hello",
        "--bucket", "hello",
        "--retention", "72h",
        "--force"
    ));

    let data = tokio::fs::read("./influx-data/configs").await.unwrap();
    let data = String::from_utf8_lossy(&data);
    let mut token = data.split("token = \"");
    token.next().unwrap();
    let token = token.next().unwrap();
    let mut token = token.split("\"");
    let token = token.next().unwrap();

    println!("GOT TOKEN: {token}");

    let client = influxdb::Client::new(format!("http://127.0.0.1:{port}"), "hello")
        .with_token(token);

    println!("{:?}", client.ping().await);

    let mut last_time = std::time::Instant::now();

    let mut did_print_example = false;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let timestamp = influxdb::Timestamp::Nanoseconds(
            std::time::SystemTime::UNIX_EPOCH
            .elapsed()
            .unwrap()
            .as_nanos()
        );

        let val = last_time.elapsed().as_secs_f64();

        let write = influxdb::WriteQuery::new(timestamp, "last-second")
            .add_field("text-field", "field-value")
            .add_field("float-field", val)
            .add_tag("text-tag", "tag-value")
            .add_tag("float-tag", val);

        if !did_print_example {
            did_print_example = true;
            println!("QUERY: {write:?}");
        }

        client.query(write).await.unwrap();

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
            if line.contains("msg=Listening") && line.contains("service=tcp-listener") && line.contains("transport=http") {
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
