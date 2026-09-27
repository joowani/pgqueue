#[pgqueue::job]
async fn ordinary(_: ()) {}

#[pgqueue::cron("* * * * *")]
async fn scheduled() {}

fn register_job_rejects_cron(builder: pgqueue::WorkerBuilder) {
    let _ = builder.register_job(scheduled);
}

fn register_cron_rejects_job(builder: pgqueue::WorkerBuilder) {
    let _ = builder.register_cron(ordinary);
}

fn schedule_cron_rejects_cron(builder: pgqueue::WorkerBuilder) {
    let _ = builder.schedule_cron("* * * * *", scheduled::job());
}

fn main() {}
